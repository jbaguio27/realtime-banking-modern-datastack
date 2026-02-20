import argparse
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any

from dotenv import load_dotenv

DEFAULT_LOOP = True
SLEEP_SECONDS = 2


@dataclass(frozen=True)
class GeneratorConfig:
    num_customers: int = 30
    accounts_per_customer: int = 2
    num_transactions: int = 400
    max_txn_amount: float = 2500.00
    currency: str = "USD"

    low_balance_min: Decimal = Decimal("10.00")
    low_balance_max: Decimal = Decimal("95.00")
    normal_balance_min: Decimal = Decimal("100.00")
    normal_balance_max: Decimal = Decimal("5000.00")
    low_balance_account_ratio: float = 0.30
    no_recent_activity_account_ratio: float = 0.20

    allow_overdrawn: bool = True
    overdrawn_account_ratio: float = 0.10
    overdrawn_min_abs: Decimal = Decimal("20.00")
    overdrawn_max_abs: Decimal = Decimal("800.00")

    dormant_customer_ratio: float = 0.25
    failed_tx_ratio: float = 0.08
    pending_tx_ratio: float = 0.03

    @staticmethod
    def from_env() -> "GeneratorConfig":
        return GeneratorConfig(
            num_customers=int(os.getenv("GEN_NUM_CUSTOMERS", "30")),
            accounts_per_customer=int(os.getenv("GEN_ACCOUNTS_PER_CUSTOMER", "2")),
            num_transactions=int(os.getenv("GEN_NUM_TRANSACTIONS", "400")),
            max_txn_amount=float(os.getenv("GEN_MAX_TXN_AMOUNT", "2500.00")),
            currency=os.getenv("GEN_CURRENCY", "USD"),
            low_balance_account_ratio=float(os.getenv("GEN_LOW_BALANCE_ACCOUNT_RATIO", "0.30")),
            no_recent_activity_account_ratio=float(
                os.getenv("GEN_NO_RECENT_ACTIVITY_ACCOUNT_RATIO", "0.20")
            ),
            allow_overdrawn=os.getenv("GEN_ALLOW_OVERDRAWN", "true").strip().lower()
            in {"1", "true", "yes", "on"},
            overdrawn_account_ratio=float(os.getenv("GEN_OVERDRAWN_ACCOUNT_RATIO", "0.10")),
            overdrawn_min_abs=Decimal(os.getenv("GEN_OVERDRAWN_MIN_ABS", "20.00")),
            overdrawn_max_abs=Decimal(os.getenv("GEN_OVERDRAWN_MAX_ABS", "800.00")),
            dormant_customer_ratio=float(os.getenv("GEN_DORMANT_CUSTOMER_RATIO", "0.25")),
            failed_tx_ratio=float(os.getenv("GEN_FAILED_TX_RATIO", "0.08")),
            pending_tx_ratio=float(os.getenv("GEN_PENDING_TX_RATIO", "0.03")),
        )


def load_generator_env() -> None:
    """
    Load generator env values.
    Priority:
    1) DATA_GENERATOR_ENV_FILE (explicit path)
    2) data-generator/.env
    3) default dotenv discovery
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    explicit_file = os.getenv("DATA_GENERATOR_ENV_FILE")
    if explicit_file:
        load_dotenv(dotenv_path=explicit_file, override=False)
        return

    default_env = os.path.join(script_dir, ".env")
    if os.path.exists(default_env):
        load_dotenv(dotenv_path=default_env, override=False)
    else:
        load_dotenv(override=False)


def validate_config(cfg: GeneratorConfig) -> None:
    if cfg.num_customers <= 0:
        raise ValueError("GEN_NUM_CUSTOMERS must be > 0")
    if cfg.accounts_per_customer <= 0:
        raise ValueError("GEN_ACCOUNTS_PER_CUSTOMER must be > 0")
    if cfg.num_transactions <= 0:
        raise ValueError("GEN_NUM_TRANSACTIONS must be > 0")
    if cfg.max_txn_amount <= 0:
        raise ValueError("GEN_MAX_TXN_AMOUNT must be > 0")

    ratio_fields = {
        "GEN_LOW_BALANCE_ACCOUNT_RATIO": cfg.low_balance_account_ratio,
        "GEN_NO_RECENT_ACTIVITY_ACCOUNT_RATIO": cfg.no_recent_activity_account_ratio,
        "GEN_OVERDRAWN_ACCOUNT_RATIO": cfg.overdrawn_account_ratio,
        "GEN_DORMANT_CUSTOMER_RATIO": cfg.dormant_customer_ratio,
        "GEN_FAILED_TX_RATIO": cfg.failed_tx_ratio,
        "GEN_PENDING_TX_RATIO": cfg.pending_tx_ratio,
    }
    for name, value in ratio_fields.items():
        if not (0.0 <= value <= 1.0):
            raise ValueError(f"{name} must be between 0 and 1")

    if cfg.overdrawn_min_abs <= 0 or cfg.overdrawn_max_abs <= 0:
        raise ValueError("GEN_OVERDRAWN_MIN_ABS and GEN_OVERDRAWN_MAX_ABS must be > 0")
    if cfg.overdrawn_min_abs > cfg.overdrawn_max_abs:
        raise ValueError("GEN_OVERDRAWN_MIN_ABS must be <= GEN_OVERDRAWN_MAX_ABS")
    if cfg.failed_tx_ratio + cfg.pending_tx_ratio > 1.0:
        raise ValueError("GEN_FAILED_TX_RATIO + GEN_PENDING_TX_RATIO must be <= 1")


def random_money(min_val: Decimal, max_val: Decimal, rng: Any = random) -> Decimal:
    val = Decimal(str(rng.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def random_tx_timestamp(rng: Any = random, now: datetime | None = None) -> datetime:
    """Recency-biased timestamp that still covers the last 90 days."""
    if now is None:
        now = datetime.now(timezone.utc)

    bucket = rng.random()
    if bucket < 0.70:
        days_back = rng.randint(0, 7)
    elif bucket < 0.90:
        days_back = rng.randint(8, 30)
    else:
        days_back = rng.randint(31, 90)

    seconds_back = rng.randint(0, 86399)
    return now - timedelta(days=days_back, seconds=seconds_back)


def random_created_at(max_days_back: int = 365, rng: Any = random) -> datetime:
    now = datetime.now(timezone.utc)
    days_back = rng.randint(0, max_days_back)
    seconds_back = rng.randint(0, 86399)
    return now - timedelta(days=days_back, seconds=seconds_back)


def choose_subset(items: list[int], ratio: float, ensure_one: bool = True, rng: Any = random) -> set[int]:
    if not items or ratio <= 0:
        return set()

    count = int(len(items) * ratio)
    if ensure_one and ratio > 0:
        count = max(1, count)
    count = min(count, len(items))
    return set(rng.sample(items, count))


def build_unique_email(base_email: str, rng: Any = random, now_ms: int | None = None) -> str:
    local, _, domain = base_email.partition("@")
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    suffix = f"{now_ms}{rng.randint(1000, 9999)}"
    return f"{local}.{suffix}@{domain or 'example.com'}"


def create_connection():
    try:
        import psycopg2  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: psycopg2. Rebuild image or install psycopg2-binary.")
        sys.exit(2)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    conn.autocommit = True
    return conn


def insert_customer_with_retry(cur, faker_obj, max_retries: int = 10) -> int:
    import psycopg2  # type: ignore

    for _ in range(max_retries):
        first_name = faker_obj.first_name()
        last_name = faker_obj.last_name()
        email = build_unique_email(faker_obj.email())
        created_at = random_created_at(720)

        try:
            cur.execute(
                """
                INSERT INTO customers (first_name, last_name, email, created_at)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (first_name, last_name, email, created_at),
            )
            return cur.fetchone()[0]
        except psycopg2.Error as exc:
            if getattr(exc, "pgcode", None) != "23505":
                raise

    raise RuntimeError("Failed to insert a unique customer email after multiple retries.")


def seed_accounts(cur, customers: list[int], cfg: GeneratorConfig):
    accounts: list[int] = []
    accounts_by_customer: dict[int, list[int]] = {}

    for customer_id in customers:
        accounts_by_customer[customer_id] = []
        for _ in range(cfg.accounts_per_customer):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            created_at = random_created_at(365)

            if random.random() < cfg.low_balance_account_ratio:
                initial_balance = random_money(cfg.low_balance_min, cfg.low_balance_max)
            else:
                initial_balance = random_money(cfg.normal_balance_min, cfg.normal_balance_max)

            cur.execute(
                """
                INSERT INTO accounts (customer_id, account_type, balance, currency, created_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (customer_id, account_type, initial_balance, cfg.currency, created_at),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)
            accounts_by_customer[customer_id].append(account_id)

    return accounts, accounts_by_customer


def seed_overdrawn_accounts(cur, account_ids: list[int], cfg: GeneratorConfig) -> tuple[int, str | None]:
    if not cfg.allow_overdrawn:
        return 0, None

    target_ids = choose_subset(account_ids, cfg.overdrawn_account_ratio)
    if not target_ids:
        return 0, None

    try:
        cur.execute(
            """
            UPDATE accounts
            SET balance = ROUND((%s + random() * (%s - %s))::numeric, 2)
            WHERE id = ANY(%s)
            """,
            (
                -float(cfg.overdrawn_max_abs),
                -float(cfg.overdrawn_min_abs),
                -float(cfg.overdrawn_max_abs),
                list(target_ids),
            ),
        )
        return cur.rowcount, None
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def seed_transactions(cur, account_ids: list[int], cfg: GeneratorConfig) -> dict[str, int]:
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    status_counts = {"COMPLETED": 0, "FAILED": 0, "PENDING": 0}

    for _ in range(cfg.num_transactions):
        account_id = random.choice(account_ids)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1, cfg.max_txn_amount), 2)

        related_account = None
        if txn_type == "TRANSFER" and len(account_ids) > 1:
            related_account = random.choice([a for a in account_ids if a != account_id])

        chance = random.random()
        if chance < cfg.failed_tx_ratio:
            status = "FAILED"
        elif chance < cfg.failed_tx_ratio + cfg.pending_tx_ratio:
            status = "PENDING"
        else:
            status = "COMPLETED"

        cur.execute(
            """
            INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (account_id, txn_type, amount, related_account, status, random_tx_timestamp()),
        )
        status_counts[status] += 1

    return status_counts


def run_iteration(cur, faker_obj, cfg: GeneratorConfig) -> None:
    customers = [insert_customer_with_retry(cur, faker_obj) for _ in range(cfg.num_customers)]
    accounts, accounts_by_customer = seed_accounts(cur, customers, cfg)

    dormant_customers = choose_subset(customers, cfg.dormant_customer_ratio)
    active_accounts = [
        account_id
        for customer_id, ids in accounts_by_customer.items()
        if customer_id not in dormant_customers
        for account_id in ids
    ]
    if not active_accounts:
        active_accounts = accounts

    no_recent_activity_accounts = choose_subset(active_accounts, cfg.no_recent_activity_account_ratio)
    tx_eligible_accounts = [a for a in active_accounts if a not in no_recent_activity_accounts]
    if not tx_eligible_accounts:
        tx_eligible_accounts = active_accounts

    overdrawn_seeded, overdrawn_seed_error = seed_overdrawn_accounts(cur, accounts, cfg)
    status_counts = seed_transactions(cur, tx_eligible_accounts, cfg)

    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE balance < 0) AS overdrawn_accounts,
            COUNT(*) FILTER (WHERE balance < 100) AS low_balance_accounts
        FROM accounts
        WHERE id = ANY(%s)
        """,
        (accounts,),
    )
    overdrawn_accounts_count, low_balance_accounts = cur.fetchone()

    print(
        "Generated customers={customers}, accounts={accounts}, transactions={txns}; "
        "status_completed={completed}, status_failed={failed}, status_pending={pending}, "
        "dormant_customers={dormant}, no_recent_activity_accounts={no_recent}, "
        "overdrawn_accounts={overdrawn}, overdrawn_seeded={seeded}, low_balance_accounts={low_bal}".format(
            customers=len(customers),
            accounts=len(accounts),
            txns=cfg.num_transactions,
            completed=status_counts["COMPLETED"],
            failed=status_counts["FAILED"],
            pending=status_counts["PENDING"],
            dormant=len(dormant_customers),
            no_recent=len(no_recent_activity_accounts),
            overdrawn=overdrawn_accounts_count,
            seeded=overdrawn_seeded,
            low_bal=low_balance_accounts,
        )
    )

    if overdrawn_seed_error:
        print(
            "Warning: could not seed overdrawn balances. "
            f"Set GEN_ALLOW_OVERDRAWN=false or remove non-negative constraint. error={overdrawn_seed_error}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run fake data generator")
    parser.add_argument("--once", action="store_true", help="Run a bounded job then exit")
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of iterations to run when --once is set",
    )
    return parser


def main() -> int:
    load_generator_env()
    cfg = GeneratorConfig.from_env()
    validate_config(cfg)

    try:
        from faker import Faker  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: faker. Rebuild image or install faker.")
        return 2

    args = build_arg_parser().parse_args()
    loop_forever = (not args.once) and DEFAULT_LOOP

    faker_obj = Faker()
    conn = create_connection()
    cur = conn.cursor()

    try:
        iteration = 0
        if args.once:
            for _ in range(args.iterations):
                iteration += 1
                print(f"\n--- Iteration {iteration} started ---")
                run_iteration(cur, faker_obj, cfg)
                print(f"--- Iteration {iteration} finished ---")
        else:
            while True:
                iteration += 1
                print(f"\n--- Iteration {iteration} started ---")
                run_iteration(cur, faker_obj, cfg)
                print(f"--- Iteration {iteration} finished ---")
                if not loop_forever:
                    break
                time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting gracefully...")
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())

