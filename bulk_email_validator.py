"""Bulk email validator via asynchronous SMTP.

Reads email addresses from a file (default: emails.txt) and validates each one by
connecting to the domain's MX server and issuing SMTP commands without sending a
message. Results are appended to an output CSV file and MX lookups are cached in
mx_cache.json for faster subsequent runs. Progress is printed as a percentage.

This version uses asyncio for non-blocking SMTP conversations, enabling high
concurrency. With a fast network connection and permissive remote servers it can
process hundreds of emails per second.

Requires the `dnspython` package.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

try:
    import dns.resolver
except ImportError:  # pragma: no cover
    raise SystemExit("dnspython is required: pip install dnspython")


async def load_emails(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def get_mx_records(domain: str) -> list[str]:
    try:
        answers = dns.resolver.resolve(domain, "MX")
        records = sorted((r.preference, str(r.exchange).rstrip(".")) for r in answers)
        return [host for _, host in records]
    except Exception:  # pragma: no cover - network failures
        # Fall back to the domain itself if MX lookup fails
        return [domain]


async def smtp_check_async(host: str, email: str, timeout: float) -> bool | None:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, 25), timeout)
        await reader.readline()  # banner
        writer.write(b"HELO validator.local\r\n")
        await writer.drain()
        await reader.readline()
        writer.write(b"MAIL FROM:<validator@local>\r\n")
        await writer.drain()
        await reader.readline()
        writer.write(f"RCPT TO:<{email}>\r\n".encode())
        await writer.drain()
        line = await reader.readline()
        code = int(line[:3])
        writer.write(b"QUIT\r\n")
        await writer.drain()
        return code in (250, 451, 452)
    except Exception:
        return None
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def validate_email(email: str, mx_cache: dict[str, list[str]], timeout: float) -> bool:
    user, domain = email.split("@", 1)
    if domain not in mx_cache:
        mx_cache[domain] = await asyncio.to_thread(get_mx_records, domain)
    hosts = mx_cache[domain]

    for host in hosts + [domain]:
        valid = await smtp_check_async(host, email, timeout)
        if valid is not None:
            return valid
    return False


async def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk email validator via SMTP")
    parser.add_argument("--input", default="emails.txt", help="File with emails, one per line")
    parser.add_argument("--output", default="validation_results.csv", help="File to append results")
    parser.add_argument("--cache", default="mx_cache.json", help="File to store MX lookup cache")
    parser.add_argument("--concurrency", type=int, default=500, help="Number of parallel validations")
    parser.add_argument("--timeout", type=float, default=10.0, help="Connection timeout in seconds")
    args = parser.parse_args()

    emails = await load_emails(Path(args.input))
    total = len(emails)
    if total == 0:
        print("No emails found.")
        return

    cache_path = Path(args.cache)
    mx_cache: dict[str, list[str]] = {}
    if cache_path.exists():
        try:
            mx_cache = json.loads(cache_path.read_text())
        except json.JSONDecodeError:
            mx_cache = {}

    processed = 0
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(args.concurrency)
    output_path = Path(args.output)
    output_file = output_path.open("a")

    async def worker(email: str) -> None:
        nonlocal processed
        async with sem:
            valid = await validate_email(email, mx_cache, args.timeout)
        async with lock:
            processed += 1
            status = "valid" if valid else "invalid"
            output_file.write(f"{email},{status}\n")
            output_file.flush()
            pct = processed * 100 / total
            print(f"\rProcessed {processed}/{total} ({pct:.1f}%)", end="")

    await asyncio.gather(*(worker(email) for email in emails))
    output_file.close()

    cache_path.write_text(json.dumps(mx_cache, indent=2))
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
