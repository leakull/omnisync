def test_celery_tasks_registered():
    import src.github.tasks  # noqa: F401
    import src.telegram.tasks  # noqa: F401
    from src.celery_app import celery_app

    task_names = set(celery_app.tasks.keys())
    assert "src.github.tasks.sync_github_commits" in task_names
    assert "src.github.tasks.sync_github_pull_requests" in task_names
    assert "src.telegram.tasks.sync_telegram_messages" in task_names


def test_main_import_does_not_carry_celery():
    import src.main

    assert not hasattr(src.main, "celery_app")
