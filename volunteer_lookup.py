import csv
from collections import defaultdict

USERS_CSV = "data/users_migrated.csv"
PARTICIPANTS_CSV = "data/opportunity_participants_migrated.csv"
OPPORTUNITIES_CSV = "data/opportunities_migrated.csv"


def load_csv(filename):
    with open(filename, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def find_volunteers_by_name(users, search_name):
    """Return all users whose name matches (case-insensitive, partial match supported)."""
    search_lower = search_name.strip().lower()
    exact = [u for u in users if u["name"].strip().lower() == search_lower]
    if exact:
        return exact
    # Fall back to partial match
    return [u for u in users if search_lower in u["name"].strip().lower()]


def main():
    users = load_csv(USERS_CSV)
    participants = load_csv(PARTICIPANTS_CSV)
    opportunities = load_csv(OPPORTUNITIES_CSV)

    # Build lookup: opportunity_id -> opportunity row
    opp_by_id = {o["id"]: o for o in opportunities}

    print("=" * 50)
    print("   Volunteer Date Lookup")
    print("=" * 50)

    while True:
        name = input("\nEnter volunteer name (or 'quit' to exit): ").strip()
        if name.lower() in ("quit", "exit", "q"):
            print("Goodbye!")
            break
        if not name:
            continue

        matched_users = find_volunteers_by_name(users, name)

        if not matched_users:
            print(f"  No user found matching '{name}'.")
            continue

        if len(matched_users) > 1:
            print(f"  Found {len(matched_users)} users matching '{name}':")
            for i, u in enumerate(matched_users, 1):
                print(f"  {i}. {u['name']} ({u['email']})")
            print("  Showing results for ALL matches.\n")

        for user in matched_users:
            user_id = user["id"]
            user_name = user["name"]
            user_email = user["email"]

            # Find all participation records for this user
            user_participations = [p for p in participants if p["user_id"] == user_id]

            print(f"\n  {user_name} ({user_email})")

            if not user_participations:
                print("    No volunteer records found.")
                continue

            print(f"    User ID: {user_id}")
            print(f"    {len(user_participations)} volunteer session(s):")
            for p in user_participations:
                opp = opp_by_id.get(p["opportunity_id"])
                if opp:
                    start = opp.get("start_time") or opp.get("datetime") or "Unknown date"
                    title = opp.get("title", "Unknown event")
                    hours = p.get("total_hours", "?")
                    print(f"    - {start[:10]}  |  {title}  |  {hours} hrs")
                    print(f"        OpPar ID:   {p['id']}")
                    print(f"        Opportunity ID:   {p['opportunity_id']}")
                else:
                    print(f"    - [opportunity {p['opportunity_id']} not found]")
                    print(f"        OpPar ID:   {p['id']}")
                    print(f"        Opportunity ID:   {p['opportunity_id']}")


if __name__ == "__main__":
    main()
