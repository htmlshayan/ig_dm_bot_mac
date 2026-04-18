"""
Persistent inbox reply queue shared by server API and bot runtime.

Backed by the database so queued jobs survive process restarts.
"""
from __future__ import annotations

from config import database

_MAX_REPLY_JOBS = 300


def _copy_job(job: dict, include_private: bool = False) -> dict:
    snapshot = dict(job or {})
    if not include_private:
        snapshot.pop("upload_path", None)
    return snapshot


def create_reply_job(
    *,
    ig_account: str,
    thread_id: int,
    thread_display_name: str,
    text_message: str,
    has_attachment: bool,
    upload_path: str,
) -> str:
    try:
        clean_thread_id = int(thread_id or 0)
    except Exception:
        clean_thread_id = 0

    return database.create_inbox_reply_job(
        ig_account=str(ig_account or "").strip().lower(),
        thread_id=clean_thread_id,
        thread_display_name=str(thread_display_name or "").strip(),
        text_message=str(text_message or "").strip(),
        has_attachment=bool(has_attachment),
        upload_path=str(upload_path or "").strip(),
        max_jobs=_MAX_REPLY_JOBS,
    )


def update_reply_job(job_id: str, **updates):
    database.update_inbox_reply_job(job_id, **updates)


def get_reply_job(job_id: str, include_private: bool = False):
    job = database.get_inbox_reply_job(job_id)
    if not job:
        return None
    return _copy_job(job, include_private=include_private)


def has_queued_reply_for_account(ig_account: str) -> bool:
    return database.has_queued_inbox_reply_for_account(ig_account)


def claim_next_reply_for_account(ig_account: str):
    selected_job = database.claim_next_inbox_reply_for_account(ig_account)
    if not selected_job:
        return None
    return _copy_job(selected_job, include_private=True)
