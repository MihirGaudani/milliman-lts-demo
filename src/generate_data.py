import json
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

random.seed(7)

OUTDIR = Path("data_raw")
OUTDIR.mkdir(parents=True, exist_ok=True)

STATES = ["CA","TX","FL","NY","IL","WA","MA","GA","NC","AZ"]
PRODUCTS = ["Term 10","Term 20","Whole Life","UL","IUL"]
STATUSES = ["Inforce","Lapsed","Surrendered","Pending","Cancelled"]
PAY_METHODS = ["ACH","Card","Check"]
CAUSES = ["Natural","Accident","Illness","Other"]

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def main(n_policies: int = 800):
    today = date.today()
    start = today - timedelta(days=365*6)

    # Policies
    policies = []
    for i in range(n_policies):
        policy_id = f"P{100000+i}"
        issue = rand_date(start, today - timedelta(days=30))
        product = random.choice(PRODUCTS)
        state = random.choice(STATES)
        face = random.choice([100000, 200000, 300000, 500000, 750000, 1000000])
        annual_prem = round(face * random.uniform(0.004, 0.02), 2)
        ph_id = f"PH{200000 + random.randint(0, int(n_policies*0.8))}"
        status = random.choices(STATUSES, weights=[70,10,8,8,4])[0]

        policies.append({
            "policy_id": policy_id,
            "issue_date": issue.isoformat(),
            "state": state,
            "product": product,
            "face_amount": face,
            "annual_premium": annual_prem,
            "policyholder_id": ph_id,
            "status": status
        })

    df_pol = pd.DataFrame(policies)

    # Premium payments (monthly-ish)
    payments = []
    for _, row in df_pol.iterrows():
        issue = date.fromisoformat(row["issue_date"])
        months = random.randint(6, 48)
        monthly = round(float(row["annual_premium"]) / 12.0, 2)

        for m in range(months):
            pay_date = issue + timedelta(days=30*m + random.randint(-3, 3))
            if pay_date >= today:
                break

            amt = monthly
            # inject messiness
            if random.random() < 0.01:
                amt = -amt  # negative (bad)
            if random.random() < 0.01:
                amt = None  # missing

            payments.append({
                "policy_id": row["policy_id"],
                "payment_date": pay_date.isoformat(),
                "amount": amt,
                "payment_method": random.choice(PAY_METHODS)
            })

            # occasional duplicate
            if random.random() < 0.01:
                payments.append({
                    "policy_id": row["policy_id"],
                    "payment_date": pay_date.isoformat(),
                    "amount": amt,
                    "payment_method": random.choice(PAY_METHODS)
                })

    df_pay = pd.DataFrame(payments)

    # Claims (JSON) - only some policies
    claims = []
    claim_count = int(n_policies * 0.10)
    claim_policies = df_pol.sample(claim_count, random_state=7)["policy_id"].tolist()

    for j, pid in enumerate(claim_policies):
        pol_issue = date.fromisoformat(df_pol.loc[df_pol["policy_id"] == pid, "issue_date"].iloc[0])
        loss = rand_date(pol_issue, today - timedelta(days=1))
        reported = loss + timedelta(days=random.randint(0, 45))
        paid = round(random.uniform(1000, 250000), 2)

        # inject issues
        if random.random() < 0.03:
            paid = -paid  # impossible
        if random.random() < 0.02:
            loss = pol_issue - timedelta(days=random.randint(1, 365))  # before issue date
        if random.random() < 0.02:
            pid_use = f"P{999999 + j}"  # unknown policy
        else:
            pid_use = pid

        claims.append({
            "claim_id": f"C{300000+j}",
            "policy_id": pid_use,
            "loss_date": loss.isoformat(),
            "reported_date": reported.isoformat(),
            "paid_amount": paid,
            "cause": random.choice(CAUSES)
        })

    # Write files
    df_pol.to_csv(OUTDIR / "policies.csv", index=False)
    df_pay.to_csv(OUTDIR / "premium_payments.csv", index=False)
    with open(OUTDIR / "claims.json", "w") as f:
        json.dump(claims, f, indent=2)

    print("Wrote:")
    print(" - data_raw/policies.csv")
    print(" - data_raw/premium_payments.csv")
    print(" - data_raw/claims.json")

if __name__ == "__main__":
    main()
