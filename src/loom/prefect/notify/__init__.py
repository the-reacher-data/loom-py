"""Notification subsystem for loom Prefect flows.

Public surface
--------------
- :class:`NotifyEvent`  — frozen payload passed to every notifier.
- :class:`Notifier`     — protocol (one method, ``notify``).
- :class:`SlackNotifier`— concrete Slack webhook implementation.
- :func:`build_notifiers` — factory that parses the YAML ``notifications:``
  block into a tuple of ``Notifier`` instances.
"""

from loom.prefect.notify._event import NotifyEvent
from loom.prefect.notify._factory import build_notifiers
from loom.prefect.notify._port import Notifier
from loom.prefect.notify._slack import SlackNotifier

__all__ = ["Notifier", "NotifyEvent", "SlackNotifier", "build_notifiers"]
