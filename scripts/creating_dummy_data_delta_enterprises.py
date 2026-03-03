import random
import pandas as pd
from faker import Faker
import os

fake = Faker()

# ======== USPS States / Bad States ========
US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC"
]
BAD_STATES = ["XX", "ZZ", "AA", "QQ"]

# ======== Helpers ========
def maybe_null(value, chance=0.1):
    return "" if random.random() < chance else value

def maybe_short(value, chance=0.15):
    value_str = str(value)
    if random.random() < chance and len(value_str) > 1:
        short_len = random.randint(1, len(value_str)-1)
        return value_str[:short_len]
    return value_str

def generate_number(length):
    return "".join(str(random.randint(0, 9)) for _ in range(length))

# ======== Donor ID ========
def generate_donor_id(min_len=5, max_len=8):
    length = random.randint(min_len, max_len)
    return "".join(str(random.randint(1, 9)) for _ in range(length))

def generate_unique_donor_ids(n):
    ids = set()
    while len(ids) < n:
        ids.add(generate_donor_id())
    return list(ids)

# ======== Clean Record Generator ========
def generate_clean_record(panel, donor_id, email_suffix):
    first = fake.first_name()
    last = fake.last_name()
    title = random.choice(["Mr.", "Ms.", "Mrs.", "Dr."])

    address1 = fake.street_address()
    address2 = random.choice(["Apt 2B", "Suite 300", "", "Floor 5"])
    if address1 == "":
        address2 = ""

    return {
        "PANEL": panel,
        "DONOR_ID": donor_id,
        "TITLE": title,
        "FIRST_NAME": first,
        "LAST_NAME": last,
        "SUFFIX": random.choice(["", "Jr.", "Sr.", "III"]),
        "GREETING_LINE": f"Dear {title} {last}",
        "COMPANY": fake.company(),
        "DEPARTMENT": random.choice(["HR", "Finance", "IT", "Research", ""]),
        "ADDRESS1": address1,
        "ADDRESS2": address2,
        "CITY": fake.city(),
        "STATE": random.choice(US_STATES),
        "ZIP5": generate_number(5),
        "ZIP4": generate_number(4),
        "EMAIL": f"{first}.{last}{email_suffix}@example.com",
        "PHONE": fake.phone_number(),
        "FAX": fake.phone_number(),
        "DONATION_AMT": round(random.uniform(5, 500), 2),
        "MAIL_CODE": random.choice(["A1", "B2", "C3", "D4"]),
        "DELIVERY_POINT": generate_number(2),
        "BARCODE_ID": generate_number(2),
        "SERVICE_TYPE_ID": generate_number(3),
        "MAILER_ID": generate_number(6),
        "SERIAL_NUM": generate_number(9),
        "CONG_DIST": str(random.randint(1, 12)),
        "CONG_NAME": f"{fake.state()}-{random.randint(1,12)}",
        "REPRESENTATIVE": fake.name(),
        "SENATOR": fake.name()
    }

# ======== Dirty Record ========
def dirty_record(record, null_chance=0.1, short_chance=0.15):
    numeric_fields = [
        "ZIP5", "ZIP4", "DELIVERY_POINT", "BARCODE_ID",
        "SERVICE_TYPE_ID", "MAILER_ID", "SERIAL_NUM"
    ]
    record = record.copy()
    for col, val in record.items():
        if col not in {"PANEL", "DONOR_ID"}:
            record[col] = maybe_null(val, chance=null_chance)
        if col in numeric_fields and record[col] != "":
            record[col] = maybe_short(record[col], chance=short_chance)
    if record["ADDRESS1"] == "":
        record["ADDRESS2"] = ""
    return record

# ======== Corruption Injectors ========
def inject_donor_id_duplicates(records, donor_id_duplicate_count):
    donor_ids = [r["DONOR_ID"] for r in records]
    for idx in random.sample(range(len(records)), donor_id_duplicate_count):
        records[idx]["DONOR_ID"] = random.choice(donor_ids)
    return records

def inject_email_duplicates(records, email_duplicate_count):
    if email_duplicate_count == 0:
        return records
    indices = random.sample(range(len(records)), email_duplicate_count * 2)
    sources = indices[:email_duplicate_count]
    targets = indices[email_duplicate_count:]
    for src_idx, tgt_idx in zip(sources, targets):
        records[tgt_idx]["EMAIL"] = records[src_idx]["EMAIL"]
    return records

def inject_record_duplicates(records, record_duplicate_count):
    for idx in random.sample(range(len(records)), record_duplicate_count):
        records[idx] = random.choice(records).copy()
    return records

def inject_bad_states(records):
    for idx in random.sample(range(len(records)), random.randint(1, 4)):
        records[idx]["STATE"] = random.choice(BAD_STATES)
    return records

# ======== Main Dataset Generator ========
def generate_dataset(panel_sizes, dirty_count=0,
                     donor_id_duplicate_count=0,
                     email_duplicate_count=0,
                     record_duplicate_count=0):
    total_records = sum(panel_sizes.values())
    donor_ids = generate_unique_donor_ids(total_records)
    donor_idx = 0
    email_suffix = 1

    records = []
    for panel, size in panel_sizes.items():
        for _ in range(size):
            r = generate_clean_record(panel, donor_ids[donor_idx], email_suffix)
            donor_idx += 1
            email_suffix += 1
            records.append(r)

    # Dirty records
    for idx in random.sample(range(total_records), min(dirty_count, total_records)):
        records[idx] = dirty_record(records[idx])

    # Inject duplicates and bad states
    records = inject_bad_states(records)
    records = inject_donor_id_duplicates(records, donor_id_duplicate_count)
    records = inject_record_duplicates(records, record_duplicate_count)
    records = inject_email_duplicates(records, email_duplicate_count)  # global duplicates

    random.shuffle(records)
    df = pd.DataFrame(records)
    return df

# ======== Save Panels ========
def save_panels(df, output_dir, base_name="Delta_Enterprises"):
    os.makedirs(output_dir, exist_ok=True)
    for panel in sorted(df["PANEL"].unique()):
        panel_df = df[df["PANEL"] == panel]
        file_path = os.path.join(output_dir, f"{base_name}_P{panel}.csv")
        panel_df.to_csv(file_path, index=False)
        print(f"✅ Panel {panel} saved: {file_path}")

# ======== Run Example ========
if __name__ == "__main__":
    panel_sizes = {1: 8000, 2: 11000, 3: 15000}

    df = generate_dataset(
        panel_sizes,
        dirty_count=90,
        donor_id_duplicate_count=9,
        email_duplicate_count=5,  # inject a few global email duplicates
        record_duplicate_count=30
    )

    output_dir = os.path.join(os.getcwd(), "data", "Delta Enterprises")
    save_panels(df, output_dir, base_name="Delta_Enterprises")

    print("🎯 Dataset generation completed.")
