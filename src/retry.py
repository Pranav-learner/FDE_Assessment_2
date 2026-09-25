"""Reusable bounded retry and failure classification layer for data pipelines.

Provides deterministic retry logic with exponential backoff for transient
failures (e.g. HTTP 500, HTTP 429, timeouts) while immediately surfacing
permanent failures (e.g. auth, schema, business validation, configuration).
"""

from dataclasses import dataclass
import time
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class RetryError(Exception):
    """Base exception for the retry layer."""
    pass


class RetryExhaustedError(RetryError):
    """Raised when an operation fails after exhausting all configured retry attempts."""

    def __init__(self, message: str, attempts: int, last_exception: Exception) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


class HttpError(Exception):
    """Represents an HTTP communication failure with status code and optional Retry-After header."""

    def __init__(
        self,
        status_code: int,
        message: str = "",
        retry_after: float | None = None,
    ) -> None:
        detail = f"HTTP {status_code}: {message}" if message else f"HTTP {status_code}"
        super().__init__(detail)
        self.status_code = status_code
        self.message = message
        self.retry_after = retry_after


class AuthenticationError(HttpError):
    """Represents an authentication or authorization failure (e.g. HTTP 401 or 403)."""

    def __init__(self, status_code: int = 401, message: str = "Unauthorized") -> None:
        super().__init__(status_code=status_code, message=message)


@dataclass
class RetryResult:
    """Encapsulates the outcome and execution metadata of a retried operation.

    Attributes:
        value: The successful return value of the operation.
        attempts: Number of execution attempts performed.
        retries_occurred: True if more than 1 attempt was required.
    """

    value: Any
    attempts: int
    retries_occurred: bool

    @property
    def result(self) -> Any:
        """Alias for value to support caller conventions."""
        return self.value

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RetryResult):
            return self.value == other.value and self.attempts == other.attempts
        return self.value == other


# Explicit classification sets
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
NON_RETRYABLE_HTTP_STATUSES = {400, 401, 403, 404, 422}

RETRYABLE_EXCEPTIONS = (
    TimeoutError,
    ConnectionError,
)


def is_retryable_error(exc: Exception) -> bool:
    """Classify whether an exception represents a transient failure eligible for retry.

    Args:
        exc: The caught exception.

    Returns:
        bool: True if transient/retryable; False if permanent or non-retryable.
    """
    if isinstance(exc, AuthenticationError):
        return False

    if isinstance(exc, HttpError):
        return exc.status_code in RETRYABLE_HTTP_STATUSES

    if hasattr(exc, "status_code"):
        code = getattr(exc, "status_code")
        if isinstance(code, int):
            return code in RETRYABLE_HTTP_STATUSES

    if isinstance(exc, RETRYABLE_EXCEPTIONS):
        return True

    # Standard non-retryable exceptions (validation, value errors, schema mismatches)
    return False


def retry_call(
    operation: Callable[[], T],
    *,
    max_attempts: int = 3,
    is_retryable: Callable[[Exception], bool] = is_retryable_error,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, Exception, float], None] | None = None,
) -> RetryResult:
    """Execute an operation with bounded retries and exponential backoff.

    Args:
        operation: Callable to execute.
        max_attempts: Maximum total attempts before raising RetryExhaustedError.
        is_retryable: Predicate function determining if an exception can be retried.
        base_delay: Initial sleep duration in seconds for exponential backoff.
        max_delay: Upper bound cap on sleep duration in seconds.
        sleep_fn: Injected sleep callable (defaults to time.sleep; pass no-op in tests).
        on_retry: Optional callback hook invoked before each retry sleep (attempt, exc, delay).

    Returns:
        RetryResult: Metadata container holding value, attempt count, and retry flag.

    Raises:
        RetryExhaustedError: When maximum attempts are reached on transient errors.
        Exception: The original exception if classified as non-retryable.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")

    attempts = 0

    while True:
        attempts += 1
        try:
            val = operation()
            return RetryResult(
                value=val,
                attempts=attempts,
                retries_occurred=(attempts > 1),
            )
        except Exception as exc:
            # Check if retryable
            if not is_retryable(exc):
                # Non-retryable: halt immediately and propagate original exception
                raise exc

            # Bounded retry: halt if attempts exhausted
            if attempts >= max_attempts:
                raise RetryExhaustedError(
                    f"Operation failed after {attempts} attempts. Last error: {exc}",
                    attempts=attempts,
                    last_exception=exc,
                ) from exc

            # Calculate backoff delay
            if isinstance(exc, HttpError) and exc.retry_after is not None and exc.retry_after > 0:
                delay = min(exc.retry_after, max_delay)
            else:
                delay = min(base_delay * (2 ** (attempts - 1)), max_delay)

            if on_retry is not None:
                on_retry(attempts, exc, delay)

            sleep_fn(delay)
