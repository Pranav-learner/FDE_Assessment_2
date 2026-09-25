"""Unit tests for Step 8F — Bounded Retry and Failure Handling."""

import time
import pytest

from src.retry import (
    retry_call,
    RetryResult,
    RetryExhaustedError,
    HttpError,
    AuthenticationError,
    is_retryable_error,
)
from src.validate import ValidationError


def test_http_500_succeeds_after_retry():
    """TEST 1: HTTP 500 transient failure succeeds on second attempt."""
    call_count = 0

    def flaky_operation():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise HttpError(status_code=500, message="Internal Server Error")
        return "success"

    res = retry_call(
        flaky_operation,
        max_attempts=3,
        sleep_fn=lambda s: None,
    )

    assert res.result == "success"
    assert res.value == "success"
    assert res.attempts == 2
    assert res.retries_occurred is True
    assert call_count == 2


def test_http_429_succeeds_after_retry():
    """TEST 2: HTTP 429 rate limit succeeds on second attempt."""
    call_count = 0

    def rate_limited_operation():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise HttpError(status_code=429, message="Too Many Requests", retry_after=2.0)
        return "data_page"

    res = retry_call(
        rate_limited_operation,
        max_attempts=3,
        sleep_fn=lambda s: None,
    )

    assert res.result == "data_page"
    assert res.attempts == 2
    assert res.retries_occurred is True
    assert call_count == 2


def test_timeout_succeeds_after_retry():
    """TEST 3: Timeout error succeeds on second attempt."""
    call_count = 0

    def timing_out_operation():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise TimeoutError("Temporary socket timeout")
        return "retrieved_payload"

    res = retry_call(
        timing_out_operation,
        max_attempts=3,
        sleep_fn=lambda s: None,
    )

    assert res.result == "retrieved_payload"
    assert res.attempts == 2
    assert res.retries_occurred is True
    assert call_count == 2


def test_authentication_failure_not_retried():
    """TEST 4: Authentication failure (HTTP 401) is non-retryable and halts immediately."""
    call_count = 0

    def auth_failing_operation():
        nonlocal call_count
        call_count += 1
        raise AuthenticationError(status_code=401, message="Invalid token")

    with pytest.raises(AuthenticationError) as exc_info:
        retry_call(
            auth_failing_operation,
            max_attempts=3,
            sleep_fn=lambda s: None,
        )

    assert exc_info.value.status_code == 401
    assert call_count == 1


def test_schema_failure_not_retried():
    """TEST 5: Schema validation failure is non-retryable and halts immediately."""
    call_count = 0

    def schema_failing_operation():
        nonlocal call_count
        call_count += 1
        raise ValidationError("Missing required trip columns: ['trip_distance']")

    with pytest.raises(ValidationError):
        retry_call(
            schema_failing_operation,
            max_attempts=3,
            sleep_fn=lambda s: None,
        )

    assert call_count == 1


def test_business_validation_failure_not_retried():
    """TEST 6: Business-rule validation failure is non-retryable and halts immediately."""
    call_count = 0

    def business_rule_failing_operation():
        nonlocal call_count
        call_count += 1
        raise ValidationError("Negative trip_distance encountered")

    with pytest.raises(ValidationError):
        retry_call(
            business_rule_failing_operation,
            max_attempts=3,
            sleep_fn=lambda s: None,
        )

    assert call_count == 1


def test_retry_exhaustion():
    """TEST 7: Persistent transient failure exhausts max attempts and raises RetryExhaustedError."""
    call_count = 0

    def persistently_failing_operation():
        nonlocal call_count
        call_count += 1
        raise HttpError(status_code=500, message="Persistent server breakdown")

    with pytest.raises(RetryExhaustedError) as exc_info:
        retry_call(
            persistently_failing_operation,
            max_attempts=3,
            sleep_fn=lambda s: None,
        )

    assert exc_info.value.attempts == 3
    assert call_count == 3
    assert isinstance(exc_info.value.last_exception, HttpError)


def test_no_infinite_retries():
    """TEST 8: Confirms that permanently failing operation is called exactly max_attempts times."""
    call_count = 0

    def failing_call():
        nonlocal call_count
        call_count += 1
        raise ConnectionError("Connection refused")

    with pytest.raises(RetryExhaustedError):
        retry_call(failing_call, max_attempts=3, sleep_fn=lambda s: None)

    assert call_count == 3


def test_backoff_does_not_delay_unit_tests():
    """TEST 9: Injected sleep function captures backoff delays without delaying test runtime."""
    call_count = 0
    delays_recorded = []

    def failing_twice():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise HttpError(status_code=503, message="Service Unavailable")
        return "success"

    start_time = time.time()
    res = retry_call(
        failing_twice,
        max_attempts=4,
        base_delay=1.0,
        sleep_fn=lambda s: delays_recorded.append(s),
    )
    elapsed = time.time() - start_time

    assert elapsed < 0.1, "Test must execute without real delay"
    assert res.result == "success"
    assert res.attempts == 3
    assert delays_recorded == [1.0, 2.0]


def test_pagination_simulation():
    """TEST 10: Instructor pagination simulation across 6 pages.

    Page 1: success
    Page 2: success
    Page 3: HTTP 500 once -> retry -> success
    Page 4: success
    Page 5: HTTP 429 once -> retry -> success
    Page 6: success
    """
    page_attempts = {p: 0 for p in range(1, 7)}
    retrieved_pages = []

    def fetch_page(page_num: int) -> dict:
        page_attempts[page_num] += 1

        if page_num == 3 and page_attempts[3] == 1:
            raise HttpError(status_code=500, message="Transient page 3 server error")

        if page_num == 5 and page_attempts[5] == 1:
            raise HttpError(status_code=429, message="Rate limit on page 5", retry_after=1.5)

        return {"page": page_num, "data": f"record_batch_{page_num}"}

    # Retrieve all 6 pages using bounded retry
    for page_num in range(1, 7):
        page_result = retry_call(
            lambda p=page_num: fetch_page(p),
            max_attempts=3,
            sleep_fn=lambda s: None,
        )
        retrieved_pages.append(page_result.value)

    # Verification: all 6 pages retrieved
    assert len(retrieved_pages) == 6
    assert [p["page"] for p in retrieved_pages] == [1, 2, 3, 4, 5, 6]

    # Verify attempt counts
    assert page_attempts[1] == 1
    assert page_attempts[2] == 1
    assert page_attempts[3] == 2  # Retried once after HTTP 500
    assert page_attempts[4] == 1
    assert page_attempts[5] == 2  # Retried once after HTTP 429
    assert page_attempts[6] == 1

    # Verify no duplication
    page_ids = [p["page"] for p in retrieved_pages]
    assert len(page_ids) == len(set(page_ids))
