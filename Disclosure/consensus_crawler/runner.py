import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from naver_wisereport import fetch_quarter_consensus
from mongo_repo import ConsensusMongoRepo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh quarter/annual consensus from WiseReport ajax endpoints.")
    parser.add_argument("--codes", default="", help="Comma-separated 6-digit stock codes.")
    parser.add_argument("--codes-file", default="", help="Optional file with one stock code per line.")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit after filtering.")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent fetch worker count.")
    return parser.parse_args()


def _load_codes(args: argparse.Namespace) -> list[str]:
    if args.codes_file.strip():
        codes = []
        with open(args.codes_file, encoding="utf-8") as fp:
            for raw in fp:
                code = "".join(ch for ch in raw if ch.isdigit()).zfill(6)
                if code and code not in codes:
                    codes.append(code)
        if args.limit and args.limit > 0:
            return codes[: args.limit]
        return codes

    if args.codes.strip():
        codes = []
        for item in args.codes.split(","):
            code = "".join(ch for ch in item if ch.isdigit()).zfill(6)
            if code and code not in codes:
                codes.append(code)
        return codes

    from universe import load_universe

    df = load_universe()
    print(f"🔎 필터링 후 종목 수: {len(df)}개")
    codes = [str(row["Code"]).zfill(6) for _, row in df.iterrows()]
    if args.limit and args.limit > 0:
        codes = codes[: args.limit]
    return codes


def _fetch_one(code: str):
    return code, fetch_quarter_consensus(code)


def main() -> None:
    args = parse_args()
    repo = ConsensusMongoRepo()
    codes = _load_codes(args)
    if not codes:
        print("⚠️ 대상 종목이 없습니다.")
        return

    ok, skip, fail = 0, 0, 0
    max_workers = max(1, min(int(args.workers), 16))
    print(f"🚀 요청 시작: {len(codes)}개 / workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, code): code for code in codes}
        for idx, future in enumerate(as_completed(futures), start=1):
            code = futures[future]
            try:
                _, consensus = future.result()
                saved = repo.upsert_today(code, consensus.to_dict())
                if saved:
                    ok += 1
                    print(
                        f"✅ [{idx}/{len(codes)}] saved {code} "
                        f"q_op={consensus.operating_profit} "
                        f"fy0_op={consensus.operating_profit_fy0} "
                        f"fy1_op={consensus.operating_profit_fy1} "
                        f"actual_op={consensus.operating_profit_actual}"
                    )
                else:
                    skip += 1
                    print(f"⚠️ [{idx}/{len(codes)}] skip {code} (missing quarter/annual consensus)")
            except Exception as exc:
                fail += 1
                print(f"❌ [{idx}/{len(codes)}] fail {code}: {exc}")

    print(f"\nDONE ok={ok} skip={skip} fail={fail}")


if __name__ == "__main__":
    start = datetime.datetime.now()
    print(f"⏱️ 시작: {start:%Y-%m-%d %H:%M:%S}")
    try:
        main()
    finally:
        end = datetime.datetime.now()
        print(f"✅ 종료: {end:%Y-%m-%d %H:%M:%S} (dur={end-start})")
