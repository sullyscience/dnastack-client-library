from unittest import TestCase

from dnastack.common.tracing import Span


class TestUnitTracing(TestCase):
    def test_happy_path(self):
        span = Span(origin=self)

        http_headers = span.create_http_headers()
        self.assertEqual(http_headers['X-B3-TraceId'], span.trace_id)
        self.assertEqual(http_headers['X-B3-SpanId'], span.span_id)
        self.assertEqual(http_headers['X-B3-Sampled'], '0')
        self.assertNotIn('X-B3-ParentSpanId', http_headers)

        with span.new_span() as span_1:
            http_headers = span_1.create_http_headers()
            self.assertEqual(http_headers['X-B3-TraceId'], span.trace_id)
            self.assertEqual(http_headers['X-B3-ParentSpanId'], span.span_id)
            self.assertEqual(http_headers['X-B3-SpanId'], span_1.span_id)
            self.assertEqual(http_headers['X-B3-Sampled'], '0')

            with span_1.new_span() as span_1_1:
                http_headers = span_1_1.create_http_headers()
                self.assertEqual(http_headers['X-B3-TraceId'], span.trace_id)
                self.assertEqual(http_headers['X-B3-ParentSpanId'], span_1.span_id)
                self.assertEqual(http_headers['X-B3-SpanId'], span_1_1.span_id)
                self.assertEqual(http_headers['X-B3-Sampled'], '0')

        with span.new_span() as span_2:
            http_headers = span_2.create_http_headers()
            self.assertEqual(http_headers['X-B3-TraceId'], span.trace_id)
            self.assertEqual(http_headers['X-B3-ParentSpanId'], span.span_id)
            self.assertEqual(http_headers['X-B3-SpanId'], span_2.span_id)
            self.assertEqual(http_headers['X-B3-Sampled'], '0')

            with span_2.new_span() as span_2_1:
                http_headers = span_2_1.create_http_headers()
                self.assertEqual(http_headers['X-B3-TraceId'], span.trace_id)
                self.assertEqual(http_headers['X-B3-ParentSpanId'], span_2.span_id)
                self.assertEqual(http_headers['X-B3-SpanId'], span_2_1.span_id)
                self.assertEqual(http_headers['X-B3-Sampled'], '0')

            with span_2.new_span() as span_2_2:
                http_headers = span_2_2.create_http_headers()
                self.assertEqual(http_headers['X-B3-TraceId'], span.trace_id)
                self.assertEqual(http_headers['X-B3-ParentSpanId'], span_2.span_id)
                self.assertEqual(http_headers['X-B3-SpanId'], span_2_2.span_id)
                self.assertEqual(http_headers['X-B3-Sampled'], '0')

        # span.print_tree()
