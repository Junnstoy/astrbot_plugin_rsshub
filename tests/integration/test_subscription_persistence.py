"""Subscription updates against a fresh SQLite database, without feed/network IO."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import pytest_asyncio
from astrbot_plugin_rsshub.src.application.commands import (
    BatchActivateCommand,
    BatchDeactivateCommand,
    SubStateCommand,
)
from astrbot_plugin_rsshub.src.domain.entities.subscription import Subscription
from astrbot_plugin_rsshub.src.infrastructure.persistence import (
    subscription_repository_impl,
)
from astrbot_plugin_rsshub.src.infrastructure.persistence.database import (
    RSSHubBaseModel,
)
from astrbot_plugin_rsshub.src.infrastructure.persistence.models import FeedORM, UserORM
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest_asyncio.fixture
async def subscription_repo(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(RSSHubBaseModel.metadata.create_all)
        async with sessions() as session:
            session.add(UserORM(id="test-user"))
            session.add(
                FeedORM(id=1, link="https://example.com/feed.xml", title="Test feed")
            )
            session.add(
                FeedORM(
                    id=2, link="https://example.com/second.xml", title="Second feed"
                )
            )
            await session.commit()
        monkeypatch.setattr(
            subscription_repository_impl,
            "get_database",
            lambda: SimpleNamespace(get_session=sessions),
        )
        yield subscription_repository_impl.SubscriptionRepositoryImpl()
    finally:
        await engine.dispose()


async def _create_subscription(repo, target_session="test:GroupMessage:123", feed_id=1):
    # Ordinary creation: no imported ID and no migrated database.
    return await repo.save(
        Subscription(
            user_id="test-user",
            feed_id=feed_id,
            target_session=target_session,
            title="Original title",
            interval=15,
            display_media=0,
        )
    )


@pytest.mark.asyncio
async def test_save_creates_then_updates_existing_subscription(subscription_repo):
    repo = subscription_repo
    created = await _create_subscription(repo)
    assert created.id is not None

    # get_by_id() returns a detached domain entity from a different session.
    loaded = await repo.get_by_id(created.id)
    loaded.title = "Updated title"
    loaded.state = 0
    saved = await repo.save(loaded)

    assert saved.id == created.id
    assert saved.title == "Updated title"
    assert saved.state == 0
    assert saved.interval == 15
    assert saved.display_media == 0
    persisted = await repo.get_by_id(created.id)
    assert persisted.model_dump() == saved.model_dump()
    assert [sub.id for sub in await repo.get_by_user("test-user")] == [created.id]

    # A further new subscription must still receive a distinct generated ID.
    second = await _create_subscription(repo, "test:GroupMessage:456")
    assert second.id is not None and second.id != created.id
    assert len(await repo.get_by_user("test-user")) == 2


@pytest.mark.asyncio
async def test_single_state_command_preserves_subscription_identity(subscription_repo):
    repo = subscription_repo
    created = await _create_subscription(repo)
    command = SubStateCommand(repo)

    for enable in (False, True, False):
        result = await command.execute(created.id, "test-user", enable)
        assert result.success
        saved = await repo.get_by_id(created.id)
        assert saved.state == int(enable)
        assert saved.interval == 15
        assert saved.title == created.title
        assert [sub.id for sub in await repo.get_by_user("test-user")] == [created.id]


@pytest.mark.asyncio
@pytest.mark.parametrize("by_session", [False, True])
async def test_batch_state_commands_update_without_duplicate_rows(
    subscription_repo, by_session
):
    repo = subscription_repo
    first = await _create_subscription(repo)
    second = await _create_subscription(repo, feed_id=2)
    original_ids = [first.id, second.id]

    for command, state in (
        (BatchDeactivateCommand(repo), 0),
        (BatchActivateCommand(repo), 1),
    ):
        if by_session:
            result = await command.execute_by_session("test-user", first.target_session)
        else:
            result = await command.execute(original_ids, "test-user")
        assert result.success
        subscriptions = await repo.get_by_user("test-user")
        assert [sub.id for sub in subscriptions] == original_ids
        assert all(sub.state == state for sub in subscriptions)
        assert all(
            sub.interval == 15 and sub.display_media == 0 for sub in subscriptions
        )
