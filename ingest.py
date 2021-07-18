import argparse
import os
from datetime import date
from json.decoder import JSONDecodeError
from typing import Dict, List

from supabase_py import Client, create_client
from tefas import Crawler


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    return args


def get_tefas_data(date: str) -> List[Dict[str, str]]:
    client = Crawler()
    data = client.fetch(date)
    data["date"] = data["date"].apply(lambda x: x.isoformat())
    data = data[["date", "code", "price"]].to_dict("records")
    return data


def get_supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    supabase = create_client(url, key)
    return supabase


def main():
    args = get_args()
    supabase = get_supabase_client()
    data = get_tefas_data(args.date)

    # Delete existing records
    res = supabase.table("prices").select("*").eq("date", date).execute()
    if res["data"]:
        # Pass JSONDecodeError silently because of a bug in supabase-py
        # https://github.com/supabase/supabase-py/issues/22
        try:
            supabase.table("prices").delete().eq("date", date).execute()
        except JSONDecodeError:
            pass

    # Insert new records
    supabase.table("prices").insert(data).execute()


if __name__ == "__main__":
    main()
