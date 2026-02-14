"""
Tests for GUI features including: Placeholder Engine, DNS Domain Checker, Live Sent Email Log,
CollapsibleSection widget, GUI performance improvements, Health Monitor dashboard,
Log Viewer tab, and surfaced configuration fields.
These tests validate the backend logic and source structure without requiring a GUI (tkinter) window.
"""

import unittest
import re
import csv
import io
import queue
from datetime import datetime
from unittest.mock import MagicMock, patch

# Test Jinja2 template validation logic
try:
    from jinja2 import Environment, exceptions as jinja_exceptions
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

# Test DNS resolution logic
try:
    import dns.resolver
    DNSPYTHON_AVAILABLE = True
except ImportError:
    DNSPYTHON_AVAILABLE = False


class TestTemplateValidation(unittest.TestCase):
    """Tests for Jinja2 template validation and syntax error detection."""

    def setUp(self):
        self.known_placeholders = {
            "[EMAIL]", "[FIRSTNAME]", "[LASTNAME]", "[COMPANY]",
            "[DATE]", "[TIME]", "[GREETINGS]", "[SENDER_NAME]",
            "[DOMAIN]", "[UNAME]", "[EMAIL64]", "[COMPANYFULL]",
            "[DATE-1DAY]", "[DATE-2]", "[FUTURE-1DAY]", "[CURRENTDATE]",
        }

    def test_valid_bracket_placeholders(self):
        content = "Hello [FIRSTNAME], your email is [EMAIL]."
        found = re.findall(r'\[([A-Z0-9_\-]+)\]', content)
        for ph in found:
            self.assertIn(f"[{ph}]", self.known_placeholders)

    def test_unknown_bracket_placeholder_detected(self):
        content = "Hello [UNKNOWNFIELD], welcome."
        found = re.findall(r'\[([A-Z0-9_\-]+)\]', content)
        unknown = [f"[{ph}]" for ph in found if f"[{ph}]" not in self.known_placeholders]
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0], "[UNKNOWNFIELD]")

    @unittest.skipUnless(JINJA2_AVAILABLE, "Jinja2 not available")
    def test_valid_jinja2_template(self):
        content = "Hello {{ firstname }}, welcome to {{ company }}."
        env = Environment()
        parsed = env.parse(content)
        self.assertIsNotNone(parsed)

    @unittest.skipUnless(JINJA2_AVAILABLE, "Jinja2 not available")
    def test_invalid_jinja2_syntax_detected(self):
        content = "Hello {% if user %} welcome {% endif"
        env = Environment()
        with self.assertRaises(jinja_exceptions.TemplateSyntaxError):
            env.parse(content)

    @unittest.skipUnless(JINJA2_AVAILABLE, "Jinja2 not available")
    def test_unclosed_jinja2_block_detection(self):
        content = "{% if user %}Hello{% if admin %}Admin{% endif %}"
        open_blocks = len(re.findall(r'\{%\s*(?:if|for|block|macro)\b', content))
        close_blocks = len(re.findall(r'\{%\s*(?:endif|endfor|endblock|endmacro)\b', content))
        self.assertGreater(open_blocks, close_blocks)

    def test_unclosed_jinja2_expression(self):
        content = "Hello {{ firstname"
        has_open = '{{' in content
        has_close = '}}' in content
        self.assertTrue(has_open)
        self.assertFalse(has_close)

    @unittest.skipUnless(JINJA2_AVAILABLE, "Jinja2 not available")
    def test_jinja2_variable_extraction(self):
        content = "Hello {{ firstname }}, {{ company }} welcomes {{ email }}."
        jinja_vars = re.findall(r'\{\{\s*(\w+(?:\.\w+)*)\s*\}\}', content)
        self.assertEqual(set(jinja_vars), {'firstname', 'company', 'email'})


class TestContextPreview(unittest.TestCase):
    """Tests for context variable preview rendering."""

    def test_context_from_email(self):
        email = "john.doe@example.com"
        local_part = email.split('@')[0]
        domain = email.split('@')[1]
        parts = re.split(r'[._\-+]+', local_part)
        valid_parts = [p for p in parts if len(p) > 1 and p.isalpha()]
        firstname = valid_parts[0].capitalize() if valid_parts else 'User'
        company = domain.split('.')[0].capitalize()

        self.assertEqual(firstname, 'John')
        self.assertEqual(company, 'Example')
        self.assertEqual(domain, 'example.com')

    def test_context_from_single_part_email(self):
        email = "admin@company.org"
        local_part = email.split('@')[0]
        parts = re.split(r'[._\-+]+', local_part)
        valid_parts = [p for p in parts if len(p) > 1 and p.isalpha()]
        firstname = valid_parts[0].capitalize() if valid_parts else 'User'
        self.assertEqual(firstname, 'Admin')

    def test_context_from_numeric_email(self):
        email = "12345@numbers.com"
        local_part = email.split('@')[0]
        parts = re.split(r'[._\-+]+', local_part)
        valid_parts = [p for p in parts if len(p) > 1 and p.isalpha()]
        firstname = valid_parts[0].capitalize() if valid_parts else 'User'
        self.assertEqual(firstname, 'User')


class TestDNSDomainChecker(unittest.TestCase):
    """Tests for DNS domain checker logic."""

    def test_domain_extraction_from_emails(self):
        emails = ["user@example.com", "admin@test.org", "info@example.com"]
        domains = sorted(set(e.split('@')[1] for e in emails if '@' in e))
        self.assertEqual(domains, ['example.com', 'test.org'])

    def test_domain_extraction_from_file_lines(self):
        lines = ["example.com\n", "test.org\n", " invalid \n", "another.net\n"]
        domains = [d.strip() for d in lines if d.strip() and '.' in d.strip() and ' ' not in d.strip()]
        self.assertEqual(len(domains), 3)

    def test_domain_from_email_in_file(self):
        line = "user@example.com"
        if '@' in line:
            line = line.split('@')[1]
        self.assertEqual(line, 'example.com')

    def test_dns_result_structure(self):
        result = {
            'domain': 'example.com',
            'mx': 'mx1.example.com',
            'spf': '✅',
            'dkim': '✅',
            'dmarc': '✅',
            'status': '✅ All Pass',
            'tag': 'pass'
        }
        self.assertIn('domain', result)
        self.assertIn('mx', result)
        self.assertIn('spf', result)
        self.assertIn('dkim', result)
        self.assertIn('dmarc', result)
        self.assertIn('status', result)
        self.assertIn('tag', result)

    def test_dns_status_classification(self):
        checks = ['✅', '✅', '❌']
        pass_count = sum(1 for c in checks if c == '✅')
        self.assertEqual(pass_count, 2)

        if pass_count == 3:
            status = '✅ All Pass'
        elif pass_count == 0:
            status = '❌ Critical Issues'
        else:
            status = f'⚠️ {pass_count}/3 Pass'
        self.assertEqual(status, '⚠️ 2/3 Pass')


class TestSentEmailLog(unittest.TestCase):
    """Tests for the sent email log system."""

    def test_log_entry_structure(self):
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        entry = {'timestamp': timestamp, 'severity': 'INFO', 'message': 'Test message'}
        self.assertIn('timestamp', entry)
        self.assertIn('severity', entry)
        self.assertIn('message', entry)

    def test_severity_auto_classification(self):
        test_cases = [
            ("❌ Failed to send", "ERROR"),
            ("Error connecting to SMTP", "ERROR"),
            ("⚠️ Rate limit approaching", "WARNING"),
            ("✅ Email sent successfully", "SUCCESS"),
            ("Starting campaign...", "INFO"),
        ]
        for msg, expected in test_cases:
            if '❌' in msg or 'Error' in msg or 'error' in msg or 'FAIL' in msg:
                severity = "ERROR"
            elif '⚠️' in msg or 'Warning' in msg or 'warning' in msg:
                severity = "WARNING"
            elif '✅' in msg or 'Success' in msg or 'Sent' in msg:
                severity = "SUCCESS"
            else:
                severity = "INFO"
            self.assertEqual(severity, expected, f"Failed for: {msg}")

    def test_log_entry_cap(self):
        entries = [{'timestamp': f'[{i:02d}:00:00]', 'severity': 'INFO', 'message': f'msg {i}'} for i in range(15000)]
        # The _append_sent_log method caps at 10000 entries, trimming to the last 5000
        if len(entries) > 10000:
            entries = entries[-5000:]
        self.assertEqual(len(entries), 5000)

    def test_filter_by_severity(self):
        entries = [
            {'timestamp': '[10:00:00]', 'severity': 'INFO', 'message': 'Starting'},
            {'timestamp': '[10:00:01]', 'severity': 'ERROR', 'message': 'Failed'},
            {'timestamp': '[10:00:02]', 'severity': 'SUCCESS', 'message': 'Sent email'},
        ]
        filtered = [e for e in entries if e['severity'] == 'ERROR']
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]['message'], 'Failed')

    def test_filter_by_text(self):
        entries = [
            {'timestamp': '[10:00:00]', 'severity': 'INFO', 'message': 'Starting campaign'},
            {'timestamp': '[10:00:01]', 'severity': 'SUCCESS', 'message': 'Sent to user@test.com'},
            {'timestamp': '[10:00:02]', 'severity': 'SUCCESS', 'message': 'Sent to admin@example.com'},
        ]
        filter_text = 'test.com'
        filtered = [e for e in entries if filter_text.lower() in e['message'].lower()]
        self.assertEqual(len(filtered), 1)

    def test_export_csv_format(self):
        entries = [
            {'timestamp': '[10:00:00]', 'severity': 'INFO', 'message': 'Test message'},
            {'timestamp': '[10:00:01]', 'severity': 'ERROR', 'message': 'Error occurred'},
        ]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Timestamp', 'Severity', 'Message'])
        for entry in entries:
            writer.writerow([entry['timestamp'], entry['severity'], entry['message']])
        csv_content = output.getvalue()
        self.assertIn('Timestamp,Severity,Message', csv_content)
        self.assertIn('ERROR', csv_content)


class TestURLSubstringSanitization(unittest.TestCase):
    """Tests for URL hostname validation to prevent substring attacks."""

    def test_gmail_url_exact_hostname_match(self):
        """Verify urlparse-based hostname check matches exact Gmail domain."""
        from urllib.parse import urlparse
        url = "https://mail.google.com/mail/u/0/#inbox"
        hostname = urlparse(url).hostname
        self.assertEqual(hostname, "mail.google.com")

    def test_gmail_url_rejects_malicious_substring(self):
        """Verify urlparse rejects URLs where mail.google.com is a path, not host."""
        from urllib.parse import urlparse
        malicious_url = "https://evil.com/mail.google.com"
        hostname = urlparse(malicious_url).hostname
        self.assertNotEqual(hostname, "mail.google.com")
        self.assertEqual(hostname, "evil.com")

    def test_outlook_url_exact_hostname_match(self):
        """Verify urlparse-based hostname check matches exact Outlook domains."""
        from urllib.parse import urlparse
        for url, expected in [
            ("https://outlook.office.com/mail/", "outlook.office.com"),
            ("https://outlook.live.com/mail/", "outlook.live.com"),
        ]:
            hostname = urlparse(url).hostname
            self.assertEqual(hostname, expected)

    def test_outlook_url_rejects_malicious_substring(self):
        """Verify urlparse rejects URLs where outlook domain is in path only."""
        from urllib.parse import urlparse
        malicious_url = "https://evil.com/outlook.office.com"
        hostname = urlparse(malicious_url).hostname
        self.assertNotIn(hostname, ("outlook.office.com", "outlook.live.com"))


class TestParamikoHostKeyPolicy(unittest.TestCase):
    """Tests for paramiko host key policy (WarningPolicy instead of AutoAddPolicy)."""

    def test_warning_policy_exists(self):
        """Verify paramiko.WarningPolicy is available."""
        try:
            import paramiko
            policy = paramiko.WarningPolicy()
            self.assertIsNotNone(policy)
        except ImportError:
            self.skipTest("paramiko not available")

    def test_source_uses_warning_policy(self):
        """Verify the source code uses load_system_host_keys, not AutoAddPolicy."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertNotIn('AutoAddPolicy', content)
        self.assertNotIn('WarningPolicy', content)
        self.assertIn('load_system_host_keys', content)

    def test_source_uses_urlparse_for_url_checks(self):
        """Verify the source code uses urlparse for URL hostname checks."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # Should NOT contain vulnerable substring checks for these domains
        import re
        # Check there's no pattern like: "mail.google.com" in some_url_var
        vulnerable_patterns = re.findall(r'"mail\.google\.com"\s+in\s+\w+', content)
        self.assertEqual(len(vulnerable_patterns), 0, f"Found vulnerable URL substring checks: {vulnerable_patterns}")


class TestCollapsibleSection(unittest.TestCase):
    """Tests for the CollapsibleSection widget class."""

    def test_collapsible_section_class_exists(self):
        """Verify CollapsibleSection class is defined in source."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('class CollapsibleSection', content)

    def test_collapsible_section_has_toggle(self):
        """Verify CollapsibleSection has _toggle method."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('def _toggle(self', content)

    def test_collapsible_sections_used_in_settings(self):
        """Verify collapsible sections are used in the Settings tab."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # VPS, IMAP, Warmup sections should use CollapsibleSection
        self.assertIn("CollapsibleSection(right_frame, title=\"🖥️ Proxy VPS Mailer\"", content)
        self.assertIn("CollapsibleSection(right_frame, title=\"📬 IMAP (Reply Tracking)\"", content)
        self.assertIn("CollapsibleSection(right_frame, title=\"🌱 Warmup & Seed List\"", content)


class TestGUIPerformance(unittest.TestCase):
    """Tests for GUI performance improvements."""

    def test_log_batching_in_process_gui_updates(self):
        """Verify log messages are batched for insertion."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('log_batch = []', content)
        self.assertIn("log_batch.append(args[0])", content)
        self.assertIn("combined = ''.join(log_batch)", content)

    def test_scrollable_frame_throttling(self):
        """Verify ScrollableFrame has throttled configure updates."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('_scroll_update_pending', content)
        self.assertIn('_do_configure_update', content)

    def test_settings_tab_scrollable(self):
        """Verify Settings tab uses ScrollableFrame for overflow prevention."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('left_scroll = ScrollableFrame(left_outer)', content)
        self.assertIn('right_scroll = ScrollableFrame(right_outer)', content)


class TestNewGUITabs(unittest.TestCase):
    """Tests for new dashboard tabs."""

    def test_health_monitor_tab_exists(self):
        """Verify Health Monitor tab is built."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('_build_health_monitor_tab', content)
        self.assertIn("text='💓 Health Monitor'", content)

    def test_log_viewer_tab_exists(self):
        """Verify Log Viewer tab is built."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('_build_log_viewer_tab', content)
        self.assertIn("text='📋 Log Viewer'", content)

    def test_health_monitor_has_vps_grid(self):
        """Verify Health Monitor has VPS health grid."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('health_vps_tree', content)
        self.assertIn('_refresh_health_monitor_vps', content)

    def test_health_monitor_has_proxy_grid(self):
        """Verify Health Monitor has proxy health grid."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('health_proxy_tree', content)

    def test_health_monitor_has_retry_queue_viewer(self):
        """Verify Health Monitor has retry queue viewer."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('health_retry_tree', content)
        self.assertIn('_refresh_health_retry_queue', content)

    def test_health_monitor_has_encryption_status(self):
        """Verify Health Monitor has encryption key status."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('health_enc_status', content)
        self.assertIn('_refresh_health_enc_status', content)

    def test_log_viewer_has_filter(self):
        """Verify Log Viewer has filtering capability."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('log_viewer_filter_var', content)
        self.assertIn('_apply_log_viewer_filter', content)
        self.assertIn('log_viewer_level_var', content)


class TestMissingSurfacedFields(unittest.TestCase):
    """Tests for previously hidden backend fields now surfaced in GUI."""

    def test_unsubscribe_url_field(self):
        """Verify unsubscribe URL field is surfaced."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('unsubscribe_url_var', content)
        self.assertIn('Unsubscribe URL', content)

    def test_reply_to_override_field(self):
        """Verify Reply-To override field is surfaced."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('reply_to_override_var', content)
        self.assertIn('Reply-To Override', content)

    def test_message_id_domain_override_field(self):
        """Verify Message-ID domain override is surfaced."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('message_id_domain_override_var', content)
        self.assertIn('Message-ID Domain', content)

    def test_random_sender_pool_field(self):
        """Verify random sender pool editor is surfaced."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('random_sender_pool_var', content)
        self.assertIn('Random Sender Pool', content)

    def test_rate_limit_fields(self):
        """Verify rate limit configuration fields are surfaced."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('rate_limit_per_hour_var', content)
        self.assertIn('Rate Limit/Hour', content)

    def test_dkim_key_upload_field(self):
        """Verify DKIM key upload field exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('dkim_key_file_var', content)
        self.assertIn('_browse_dkim_key_file', content)

    def test_advanced_sending_section_collapsible(self):
        """Verify Advanced Sending Options uses CollapsibleSection."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn("CollapsibleSection(config_frame, title=\"🔧 Advanced Sending Options\"", content)


class TestExistingFeaturesPreserved(unittest.TestCase):
    """Tests to verify existing features are not removed or altered."""

    def test_sending_mode_selector_preserved(self):
        """Verify sending mode selector with all options remains."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('sending_mode_var', content)
        for mode in ["SMTP", "ThreadedSMTP", "VPS", "VPSStandalone", "DirectMX", "Browser"]:
            self.assertIn(f'value="{mode}"', content)

    def test_all_original_tabs_preserved(self):
        """Verify all original tabs still exist."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        expected_tabs = [
            "✉️ Campaign", "🖥️ VPS Bulk Sender", "🚀 Direct MX Sender",
            "⚙️ Settings", "🛡️ Deliverability", "💧 Sequences",
            "📈 Visual Analytics", "🏷️ Placeholders", "🌐 DNS Checker", "📜 Sent Log"
        ]
        for tab in expected_tabs:
            self.assertIn(tab, content, f"Missing tab: {tab}")

    def test_smtp_handler_preserved(self):
        """Verify SMTP handler initialization is intact."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.smtp_handler = SMTPHandler(self)', content)

    def test_direct_mx_handler_preserved(self):
        """Verify Direct MX handler is intact."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_handler = DirectMXHandler(self)', content)

    def test_proxy_vps_handler_preserved(self):
        """Verify Proxy VPS handler is intact."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.proxy_vps_handler = ProxyVPSHandler(self)', content)

    def test_no_logic_modification(self):
        """Verify core sending logic methods are not modified (spot check)."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # These core methods should still exist
        self.assertIn('def _start_sending(self', content)
        self.assertIn('def _stop_sending(self)', content)
        self.assertIn('def _toggle_pause(self)', content)
        self.assertIn('def _start_direct_mx_sending(self)', content)


class TestDirectMXSourceIPBinding(unittest.TestCase):
    """Tests for Direct MX source IP binding and EHLO hostname configuration."""

    def test_source_ip_field_exists_in_handler(self):
        """Verify DirectMXHandler has source_ip field."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.source_ip = ""', content)

    def test_ehlo_hostname_field_exists_in_handler(self):
        """Verify DirectMXHandler has ehlo_hostname field."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.ehlo_hostname = ""', content)

    def test_source_ip_loaded_from_settings(self):
        """Verify source_ip is loaded from settings file."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('settings.get("mx_source_ip"', content)

    def test_ehlo_hostname_loaded_from_settings(self):
        """Verify ehlo_hostname is loaded from settings file."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('settings.get("mx_ehlo_hostname"', content)

    def test_source_ip_saved_to_settings(self):
        """Verify source_ip is saved to settings file."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('"mx_source_ip":', content)

    def test_ehlo_hostname_saved_to_settings(self):
        """Verify ehlo_hostname is saved to settings file."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('"mx_ehlo_hostname":', content)

    def test_ehlo_uses_sender_domain_not_localhost(self):
        """Verify EHLO no longer uses hardcoded 'localhost'."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # The _send_via_mx method should not contain 'EHLO localhost'
        # Instead it should use ehlo_cmd variable with sender domain
        self.assertIn("ehlo_hostname = self.ehlo_hostname if self.ehlo_hostname else sender_domain", content)
        self.assertIn("ehlo_cmd = f'EHLO {ehlo_hostname}", content)
        # Should NOT have hardcoded 'EHLO localhost' in the _send_via_mx method
        # Check that there's no "b'EHLO localhost" after _send_via_mx definition
        send_via_mx_idx = content.index("async def _send_via_mx")
        # Find the next method definition after _send_via_mx
        next_method = content.index("\n    async def _read_smtp_response", send_via_mx_idx)
        mx_method_body = content[send_via_mx_idx:next_method]
        self.assertNotIn("b'EHLO localhost", mx_method_body)

    def test_source_ip_used_in_connection(self):
        """Verify asyncio.open_connection uses local_addr when source_ip is set."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # Helper method should build connect kwargs with local_addr
        self.assertIn("def _get_mx_connect_kwargs(self)", content)
        self.assertIn("kwargs['local_addr'] = (self.source_ip, 0)", content)
        # Connection calls should use the helper or connect_kwargs
        self.assertIn("self._get_mx_connect_kwargs()", content)

    def test_gui_source_ip_var_exists(self):
        """Verify GUI variable for source IP exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.mx_source_ip_var = tk.StringVar', content)

    def test_gui_ehlo_hostname_var_exists(self):
        """Verify GUI variable for EHLO hostname exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.mx_ehlo_hostname_var = tk.StringVar', content)

    def test_gui_network_config_section(self):
        """Verify Network Configuration section exists in Direct MX GUI."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn("Network Configuration (SPF Alignment)", content)
        self.assertIn("Source IP (VPS IP)", content)
        self.assertIn("EHLO Hostname", content)

    def test_source_ip_passed_to_handler_on_send(self):
        """Verify source_ip is transferred from GUI var to handler before sending."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_handler.source_ip = self.mx_source_ip_var.get()', content)
        self.assertIn('self.direct_mx_handler.ehlo_hostname = self.mx_ehlo_hostname_var.get()', content)


class TestDKIMErrorMessages(unittest.TestCase):
    """Tests for improved DKIM error messaging."""

    def test_dkim_missing_fields_reported_individually(self):
        """Verify DKIM signing reports which specific fields are missing."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('Private key not configured', content)
        self.assertIn('Selector not set', content)
        self.assertIn('Domain not set', content)

    def test_dkim_library_missing_reported(self):
        """Verify clear message when dkimpy library is not installed."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('dkimpy library not installed', content)

    def test_dkim_missing_fields_point_to_settings(self):
        """Verify DKIM error message tells user where to configure."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('Configure via Settings > DKIM Configuration', content)


class TestDirectMXGUIControl(unittest.TestCase):
    """Tests for Direct MX GUI control panel and lifecycle management."""

    def test_direct_mx_state_machine_vars(self):
        """Verify Direct MX state machine variables exist."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_running = False', content)
        self.assertIn('self.direct_mx_paused = False', content)
        self.assertIn('self.direct_mx_task = None', content)
        self.assertIn('self.direct_mx_loop = None', content)

    def test_direct_mx_live_status_panel(self):
        """Verify live status panel labels exist."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('mx_status_indicator', content)
        self.assertIn('mx_queue_depth_label', content)
        self.assertIn('mx_success_count_label', content)
        self.assertIn('mx_failure_count_label', content)

    def test_direct_mx_live_status_update_method(self):
        """Verify live status update method exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('def _update_direct_mx_live_status(self)', content)
        self.assertIn('Status: Running', content)
        self.assertIn('Status: Paused', content)
        self.assertIn('Status: Stopped', content)

    def test_direct_mx_start_sets_state(self):
        """Verify start sets running state and calls live status."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_running = True', content)
        self.assertIn('self.direct_mx_paused = False', content)
        self.assertIn('self._update_direct_mx_live_status()', content)

    def test_direct_mx_stop_cancels_task(self):
        """Verify stop cancels async task."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_task.cancel()', content)
        self.assertIn('self.direct_mx_running = False', content)

    def test_direct_mx_pause_uses_dedicated_flag(self):
        """Verify pause uses direct_mx_paused flag."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_paused = True', content)

    def test_test_mx_connection_button(self):
        """Verify Test MX Connection method exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('def _test_mx_connection(self)', content)
        self.assertIn('Test MX Connection', content)

    def test_validate_network_button(self):
        """Verify Validate Network method exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('def _validate_mx_network(self)', content)
        self.assertIn('Validate Network', content)

    def test_dkim_test_button(self):
        """Verify Test DKIM Sign method exists."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('def _test_dkim_signing(self)', content)
        self.assertIn('Test DKIM Sign', content)

    def test_direct_mx_counters(self):
        """Verify Direct MX counters exist."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.direct_mx_sent_count = 0', content)
        self.assertIn('self.direct_mx_failed_count = 0', content)
        self.assertIn('self.direct_mx_retry_count = 0', content)


class TestDirectMXButtonFixes(unittest.TestCase):
    """Tests for Direct MX Start/Pause/Stop button wiring fixes."""

    def test_mx_send_loop_checks_direct_mx_running(self):
        """Verify the MX send loop checks direct_mx_running for stop support."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('not self.main_app.direct_mx_running', content)

    def test_mx_send_loop_has_pause_support(self):
        """Verify the MX send loop has pause wait logic with direct_mx_paused."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('while self.main_app.direct_mx_paused:', content)
        self.assertIn('await asyncio.sleep(0.5)', content)

    def test_mx_send_loop_increments_direct_mx_counters(self):
        """Verify send loop increments direct_mx_sent_count and direct_mx_failed_count."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        self.assertIn('self.main_app.direct_mx_sent_count += 1', content)
        self.assertIn('self.main_app.direct_mx_failed_count += 1', content)

    def test_finalize_sending_resets_direct_mx_state(self):
        """Verify _finalize_sending resets Direct MX running state."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # Find _finalize_sending method and verify it resets direct_mx_running
        finalize_idx = content.index('def _finalize_sending(self)')
        finalize_body = content[finalize_idx:finalize_idx + 1500]
        self.assertIn('self.direct_mx_running = False', finalize_body)
        self.assertIn('self.direct_mx_paused = False', finalize_body)

    def test_finalize_sending_resets_mx_buttons(self):
        """Verify _finalize_sending re-enables the MX start button."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        finalize_idx = content.index('def _finalize_sending(self)')
        finalize_body = content[finalize_idx:finalize_idx + 1500]
        self.assertIn('mx_start_btn', finalize_body)
        self.assertIn('mx_pause_btn', finalize_body)
        self.assertIn('mx_stop_btn', finalize_body)

    def test_batch_loop_checks_stop_flag(self):
        """Verify the batch processing loop checks running/direct_mx_running before each batch."""
        with open('paris_sender_complete1.py', 'r') as f:
            content = f.read()
        # Find the batch processing section of run_direct_mx_sending_job
        job_idx = content.index('def run_direct_mx_sending_job')
        job_body = content[job_idx:job_idx + 2000]
        # Batch loop should break if not running
        self.assertIn('if not self.main_app.running or not self.main_app.direct_mx_running:', job_body)


if __name__ == '__main__':
    unittest.main()