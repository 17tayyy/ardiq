"""Registering two tasks under one name must fail loudly, not overwrite."""

import pytest


async def test_two_tasks_cannot_share_a_name(make_app):
    app = make_app("reg_dupe")

    @app.task()
    def forward_message_to_backend():
        return "whatsapp"

    with pytest.raises(ValueError, match="already registered"):

        @app.task(name="forward_message_to_backend")
        def twilio_version():
            return "twilio"

    # the first one is still there, untouched
    assert app.tasks == ["forward_message_to_backend"]
    assert app._registry["forward_message_to_backend"].fn() == "whatsapp"


async def test_the_error_names_the_module_that_owns_it(make_app):
    app = make_app("reg_owner")

    @app.task()
    def collide():
        pass

    with pytest.raises(ValueError) as excinfo:

        @app.task(name="collide")
        def other():
            pass

    assert __name__ in str(excinfo.value)  # points at where the first one lives


async def test_a_cron_cannot_shadow_a_task(make_app):
    app = make_app("reg_cron_over_task")

    @app.task()
    def nightly():
        pass

    with pytest.raises(ValueError, match="already registered"):

        @app.cron(every=60, name="nightly")
        def scheduled():
            pass

    assert app.crons == []


async def test_a_task_cannot_shadow_a_cron(make_app):
    app = make_app("reg_task_over_cron")

    @app.cron(every=60)
    def heartbeat():
        pass

    with pytest.raises(ValueError, match="already registered"):

        @app.task(name="heartbeat")
        def impostor():
            pass


async def test_an_explicit_name_disambiguates(make_app):
    """The documented way out: same function name, different task names."""
    app = make_app("reg_explicit")

    @app.task(name="whatsapp.forward")
    def forward():
        return "whatsapp"

    @app.task(name="twilio.forward")
    def forward_twilio():
        return "twilio"

    assert sorted(app.tasks) == ["twilio.forward", "whatsapp.forward"]


async def test_separate_apps_keep_separate_registries(make_app):
    app = make_app("reg_a")
    other = make_app("reg_b")

    @app.task()
    def shared():
        pass

    @other.task(name="shared")
    def also_shared():
        pass

    assert app.tasks == other.tasks == ["shared"]
