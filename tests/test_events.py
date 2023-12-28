from typing import List
from unittest import TestCase

from dnastack.common.events import EventSource, EventTypeNotRegistered


class TestUnit(TestCase):
    def test_dynamic_event_source(self):
        dynamic_event_source = EventSource()
        events: List[str] = []

        def handle_event(event):
            events.append(event.details['content'])

        # Add event handlers
        dynamic_event_source.on('alpha', handle_event)
        dynamic_event_source.on('alpha', handle_event)
        dynamic_event_source.on('bravo', handle_event)
        dynamic_event_source.on('charlie', handle_event)
        dynamic_event_source.on('delta', handle_event)

        # Remove one event handler
        dynamic_event_source.off('delta', handle_event)

        # Dispatch events
        for event_type in ['alpha', 'bravo', 'charlie', 'delta', 'echo']:
            dynamic_event_source.dispatch(event_type, dict(content=event_type))

        # Expect content from still-listening events
        self.assertIn('alpha', events)
        self.assertIn('bravo', events)
        self.assertIn('charlie', events)

        # The removed handler should not receive the event dispatch.
        self.assertNotIn('delta', events)

        # The unhandled event should not be received.
        self.assertNotIn('echo', events)

    def test_fixed_event_source(self):
        fixed_event_source = EventSource(['alpha', 'bravo', 'charlie', 'delta', 'echo'], origin=self)
        events: List[str] = []

        def handle_event(event):
            events.append(event.details['content'])

        # Add event handlers
        fixed_event_source.on('alpha', handle_event)
        fixed_event_source.on('bravo', handle_event)
        fixed_event_source.on('charlie', handle_event)
        fixed_event_source.on('delta', handle_event)

        # Remove one event handler
        fixed_event_source.off('delta', handle_event)

        # Attempt to listen to unregistered events
        with self.assertRaises(EventTypeNotRegistered):
            fixed_event_source.on('foxtrot', handle_event)

        # Dispatch events
        for event_type in ['alpha', 'bravo', 'charlie', 'delta', 'echo']:
            fixed_event_source.dispatch(event_type, dict(content=event_type))

        # Attempt to dispatch to unregistered events
        with self.assertRaises(EventTypeNotRegistered):
            fixed_event_source.dispatch('foxtrot', dict())

        # Expect content from still-listening events
        self.assertIn('alpha', events)
        self.assertIn('bravo', events)
        self.assertIn('charlie', events)

        # The removed handler should not receive the event dispatch.
        self.assertNotIn('delta', events)

        # The unhandled event should not be received.
        self.assertNotIn('echo', events)