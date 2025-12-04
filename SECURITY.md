# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security seriously in Kuba TSDB. If you discover a security vulnerability, please report it responsibly.

### How to Report

1. **Do NOT open a public GitHub issue** for security vulnerabilities
2. Email security concerns to: security@hpa.com
3. Include the following information:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your report within 48 hours
- **Assessment**: We will assess the vulnerability and determine its severity within 7 days
- **Resolution**: Critical vulnerabilities will be addressed as quickly as possible
- **Disclosure**: We will coordinate with you on public disclosure timing

### Scope

The following are in scope for security reports:

- **Injection vulnerabilities**: SQL injection, command injection, path traversal
- **Authentication/Authorization bypass**
- **Data exposure**: Unintended data leaks, improper access controls
- **Denial of Service**: Resource exhaustion, crash vulnerabilities
- **Memory safety issues**: Buffer overflows, use-after-free (in unsafe code)
- **Cryptographic weaknesses**

### Out of Scope

- Vulnerabilities in dependencies (report these upstream)
- Issues requiring physical access to the server
- Social engineering attacks
- Denial of service through excessive legitimate requests

## Security Measures

Kuba TSDB implements the following security measures:

### Input Validation
- Path traversal prevention in file operations
- Input sanitization for metric names and tags
- Redis key injection prevention

### Rate Limiting
- Per-client rate limiting using the Governor crate
- Configurable request limits
- Protection against single-client DoS

### Network Security
- TLS support for Redis connections (optional feature)
- Secure defaults for network listeners
- Timeout validation to prevent resource exhaustion

### Data Integrity
- CRC checksums on all stored chunks
- Integrity verification on read operations
- Corruption detection and recovery tools

### Build Security
- Regular dependency audits
- CodeQL analysis in CI/CD pipeline
- No test-only code in production builds

## Security Best Practices for Deployment

### Network
- Run behind a reverse proxy (nginx, HAProxy) in production
- Use TLS for all external connections
- Restrict network access to trusted clients

### File System
- Run with minimal required permissions
- Store data directory outside web-accessible paths
- Use appropriate file permissions (600 for sensitive configs)

### Redis (if enabled)
- Use Redis authentication (requirepass)
- Enable TLS for Redis connections
- Restrict Redis network access

### Monitoring
- Enable Prometheus metrics for security monitoring
- Monitor for unusual query patterns
- Set up alerting for failed authentication attempts

## Acknowledgments

We thank all security researchers who responsibly disclose vulnerabilities. Contributors who report valid security issues will be acknowledged (with permission) in release notes.
