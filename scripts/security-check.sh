#!/usr/bin/env bash
# scripts/security-check.sh
# Local SAST script for Rust applications
# Run before pushing to catch security issues early

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track overall status
FAILED_CHECKS=()
WARNINGS=()

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          🔐 Rust Security Audit - Local Check               ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to print section headers
section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}▸ $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Function to check if a command exists
check_tool() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${YELLOW}⚠ $1 not found. Installing...${NC}"
        cargo install "$1" --locked
    fi
}

# Function to run a check
run_check() {
    local name="$1"
    local cmd="$2"
    
    echo -e "${BLUE}Running: $name${NC}"
    if eval "$cmd"; then
        echo -e "${GREEN}✓ $name passed${NC}"
        return 0
    else
        echo -e "${RED}✗ $name failed${NC}"
        FAILED_CHECKS+=("$name")
        return 1
    fi
}

# ============================================================================
# INSTALL REQUIRED TOOLS
# ============================================================================
section "Checking required tools"

TOOLS=(
    "cargo-audit"
    "cargo-deny"
    "cargo-geiger"
    "cargo-outdated"
)

for tool in "${TOOLS[@]}"; do
    check_tool "$tool"
done

# ============================================================================
# VULNERABILITY AUDIT
# ============================================================================
section "Vulnerability Audit (cargo-audit)"

if ! cargo audit --deny warnings 2>&1; then
    FAILED_CHECKS+=("Vulnerability Audit")
    echo -e "${RED}✗ Vulnerabilities detected!${NC}"
else
    echo -e "${GREEN}✓ No known vulnerabilities${NC}"
fi

# ============================================================================
# DEPENDENCY CHECKS
# ============================================================================
section "Dependency Analysis (cargo-deny)"

echo "Checking advisories..."
if ! cargo deny check advisories 2>&1; then
    FAILED_CHECKS+=("Advisory Check")
fi

echo ""
echo "Checking licenses..."
if ! cargo deny check licenses 2>&1; then
    FAILED_CHECKS+=("License Check")
fi

echo ""
echo "Checking banned dependencies..."
if ! cargo deny check bans 2>&1; then
    WARNINGS+=("Banned Dependencies")
fi

echo ""
echo "Checking sources..."
if ! cargo deny check sources 2>&1; then
    FAILED_CHECKS+=("Source Check")
fi

# ============================================================================
# UNSAFE CODE ANALYSIS
# ============================================================================
section "Unsafe Code Analysis (cargo-geiger)"

echo "Analyzing unsafe code usage..."
cargo geiger --all-features --all-targets 2>&1 || true

# Count unsafe usage
UNSAFE_REPORT=$(cargo geiger --output-format Json 2>/dev/null || echo '{}')
UNSAFE_FUNCTIONS=$(echo "$UNSAFE_REPORT" | jq '[.packages[]?.unsafety.used.functions.unsafe // 0] | add // 0' 2>/dev/null || echo "0")

if [ "$UNSAFE_FUNCTIONS" -gt 0 ]; then
    echo -e "${YELLOW}⚠ Found $UNSAFE_FUNCTIONS unsafe function calls${NC}"
    WARNINGS+=("Unsafe code detected: $UNSAFE_FUNCTIONS functions")
else
    echo -e "${GREEN}✓ No unsafe code in project${NC}"
fi

# ============================================================================
# SECURITY-FOCUSED CLIPPY
# ============================================================================
section "Security Linting (Clippy)"

# Security-focused clippy flags
# Focus on blocking issues only; informational warnings don't fail the build
# Note: We only lint the library (--lib) to avoid false positives in test code
CLIPPY_FLAGS=(
    # Block on true security risks
    "-D" "clippy::mem_forget"
    "-D" "clippy::multiple_unsafe_ops_per_block"
    "-D" "clippy::undocumented_unsafe_blocks"
    # Block on debug artifacts that shouldn't be in production
    "-D" "clippy::todo"
    "-D" "clippy::unimplemented"
    "-D" "clippy::dbg_macro"
    # Warn but don't block on unwrap/expect (too many existing uses)
    "-W" "clippy::unwrap_used"
    "-W" "clippy::expect_used"
    "-W" "clippy::panic"
    # Allow common patterns that are safe
    "-A" "clippy::module_name_repetitions"
    "-A" "clippy::must_use_candidate"
    "-A" "clippy::missing_errors_doc"
    "-A" "clippy::missing_panics_doc"
    "-A" "clippy::doc_markdown"
)

# Only lint the library code (not tests, benches, examples)
if ! cargo clippy --lib --all-features -- "${CLIPPY_FLAGS[@]}" 2>&1; then
    FAILED_CHECKS+=("Clippy Security Lints")
    echo -e "${RED}✗ Security lint issues found${NC}"
else
    echo -e "${GREEN}✓ All security lints passed${NC}"
fi

# ============================================================================
# OUTDATED DEPENDENCIES
# ============================================================================
section "Outdated Dependencies"

echo "Checking for outdated dependencies..."
cargo outdated --root-deps-only 2>&1 || true

OUTDATED_COUNT=$(cargo outdated --root-deps-only --format json 2>/dev/null | jq '.dependencies | length' 2>/dev/null || echo "0")
if [ "$OUTDATED_COUNT" -gt 0 ]; then
    WARNINGS+=("$OUTDATED_COUNT outdated dependencies")
    echo -e "${YELLOW}⚠ $OUTDATED_COUNT dependencies can be updated${NC}"
fi

# ============================================================================
# SECRET SCANNING
# ============================================================================
section "Secret Scanning"

echo "Scanning for potential secrets..."
SECRET_PATTERNS=(
    'password\s*=\s*["\047][^"\047]+'
    'api[_-]?key\s*=\s*["\047][^"\047]+'
    'secret\s*=\s*["\047][^"\047]+'
    'token\s*=\s*["\047][^"\047]+'
    'BEGIN RSA PRIVATE KEY'
    'BEGIN EC PRIVATE KEY'
    'BEGIN OPENSSH PRIVATE KEY'
    'BEGIN PRIVATE KEY'
    'AWS_ACCESS_KEY_ID'
    'AWS_SECRET_ACCESS_KEY'
)

# Exclusion patterns for legitimate code (type names, imports, error variants)
EXCLUSIONS=(
    'PrivateKeyDer'           # Rust TLS type
    'PrivateKey\('            # Error variant or type constructor
    'load_private_key'        # Function name
    'use.*PrivateKey'         # Import statement
    '::PrivateKey'            # Type path
)

# Build exclusion grep pattern
EXCLUSION_PATTERN=$(IFS='|'; echo "${EXCLUSIONS[*]}")

SECRETS_FOUND=0
for pattern in "${SECRET_PATTERNS[@]}"; do
    # Filter out legitimate code patterns using exclusions
    MATCHES=$(grep -rniE "$pattern" --include="*.rs" --include="*.toml" --include="*.env*" . 2>/dev/null | grep -v target/ | grep -v ".git/" | grep -vE "$EXCLUSION_PATTERN" || true)
    if [ -n "$MATCHES" ]; then
        echo "$MATCHES"
        SECRETS_FOUND=$((SECRETS_FOUND + 1))
    fi
done

if [ "$SECRETS_FOUND" -gt 0 ]; then
    echo -e "${RED}✗ Potential secrets detected! Review the matches above.${NC}"
    FAILED_CHECKS+=("Secret Scanning")
else
    echo -e "${GREEN}✓ No obvious secrets found${NC}"
fi

# ============================================================================
# SUMMARY
# ============================================================================
section "Security Audit Summary"

echo ""
if [ ${#WARNINGS[@]} -gt 0 ]; then
    echo -e "${YELLOW}⚠ Warnings:${NC}"
    for warning in "${WARNINGS[@]}"; do
        echo -e "  ${YELLOW}• $warning${NC}"
    done
    echo ""
fi

if [ ${#FAILED_CHECKS[@]} -eq 0 ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║          ✓ All security checks passed!                       ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║          ✗ Security issues found!                            ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${RED}Failed checks:${NC}"
    for check in "${FAILED_CHECKS[@]}"; do
        echo -e "  ${RED}• $check${NC}"
    done
    echo ""
    echo -e "${YELLOW}Please fix the issues above before pushing.${NC}"
    exit 1
fi