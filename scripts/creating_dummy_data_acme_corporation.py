import random
import pandas as pd
from faker import Faker
import os

fake = Faker()

# =========================
# USPS States / Bad States
# =========================
US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC"
]
BAD_STATES = ["XX", "ZZ", "AA", "QQ"]

# =========================
# Helpers
# =========================
def maybe_null(value, chance=0.05):
    return "" if random.random() < chance else value

def maybe_short(value, expected_len, chance=0.15):
    value = str(value)
    if random.random() < chance and len(value) > 1:
        return value[:random.randint(1, expected_len - 1)]
    return value

def generate_number(length):
    return "".join(str(random.randint(0, 9)) for _ in range(length))

# =========================
# DonorID
# =========================
def generate_donor_id(min_len=5, max_len=8):
    length = random.randint(min_len, max_len)
    return "".join(str(random.randint(1, 9)) for _ in range(length))

def generate_unique_donor_ids(n):
    ids = set()
    while len(ids) < n:
        ids.add(generate_donor_id())
    return list(ids)

# =========================
# Clean Record Generator
# =========================
def generate_clean_record(panel, donor_id, unique_email_suffix):
    first = fake.first_name()
    last = fake.last_name()
    address1 = fake.street_address()
    address2 = random.choice(["Apt 2B", "Suite 300", "", "Floor 5"])
    if address1 == "":
        address2 = ""
    return {
        "panel": panel,
        "donor_id": donor_id,
        "title": random.choice(["Mr.", "Ms.", "Mrs.", "Dr."]),
        "first_name": first,
        "last_name": last,
        "suffix": random.choice(["", "Jr.", "Sr.", "III"]),
        "company": fake.company(),
        "department": random.choice(["HR", "Finance", "IT", "Research", ""]),
        "address_line_1": address1,
        "address_line_2": address2,
        "city": fake.city(),
        "state": random.choice(US_STATES),
        "zip5": generate_number(5),
        "zip4": generate_number(4),
        "email": f"{first}.{last}{unique_email_suffix}@example.com",
        "phone": fake.phone_number(),
        "fax": fake.phone_number(),
        "donation_amount": round(random.uniform(5, 500), 2),
        "mail_code": random.choice(["A1", "B2", "C3", "D4"]),
        "delivery_point": generate_number(2),
        "barcode_identifier": generate_number(2),
        "service_type_id": generate_number(3),
        "mailer_id": generate_number(6),
        "serial_number": generate_number(9),
        "congressional_district": str(random.randint(1, 12)),
        "congressional_name": f"{random.choice(US_STATES)}-{random.randint(1,12)}",
        "representative": fake.name(),
        "senator": fake.name()
    }

# =========================
# Dirty Record
# =========================
def dirty_record(record, null_chance=0.05, short_chance=0.15):
    expected_lengths = {
        "zip5": 5,
        "zip4": 4,
        "delivery_point": 2,
        "barcode_identifier": 2,
        "service_type_id": 3,
        "mailer_id": 6,
        "serial_number": 9
    }
    record = record.copy()
    for col in record:
        if col not in {"panel", "donor_id"}:
            record[col] = maybe_null(record[col], null_chance)
        if col in expected_lengths and record[col] != "":
            record[col] = maybe_short(record[col], expected_lengths[col], short_chance)
    if record["address_line_1"] == "":
        record["address_line_2"] = ""
    return record

# =========================
# Corruption Injectors
# =========================
def inject_bad_states(records, min_bad=1, max_bad=4):
    for idx in random.sample(range(len(records)), random.randint(min_bad, max_bad)):
        records[idx]["state"] = random.choice(BAD_STATES)
    return records

def inject_donor_id_duplicates(records, duplicate_count):
    donor_ids = [r["donor_id"] for r in records]
    for idx in random.sample(range(len(records)), duplicate_count):
        records[idx]["donor_id"] = random.choice(donor_ids)
    return records

def inject_record_duplicates(records, duplicate_count):
    for idx in random.sample(range(len(records)), duplicate_count):
        records[idx] = random.choice(records).copy()
    return records

def inject_email_duplicates(records, duplicate_count):
    if duplicate_count == 0:
        return records
    indices = random.sample(range(len(records)), duplicate_count * 2)
    sources = indices[:duplicate_count]
    targets = indices[duplicate_count:]
    for src_idx, tgt_idx in zip(sources, targets):
        records[tgt_idx]["email"] = records[src_idx]["email"]
    return records

# =========================
# Dataset Generator
# =========================
def generate_large_dataset(panel_sizes, dirty_count=0,
                           donor_id_duplicate_count=0,
                           email_duplicate_count=0,
                           record_duplicate_count=0):
    total_records = sum(panel_sizes.values())
    donor_ids = generate_unique_donor_ids(total_records)

    records = []
    donor_idx = 0
    email_suffix = 1

    # Generate clean records
    for panel, size in panel_sizes.items():
        for _ in range(size):
            r = generate_clean_record(panel, donor_ids[donor_idx], email_suffix)
            donor_idx += 1
            email_suffix += 1
            records.append(r)

    # Apply dirty records
    for idx in random.sample(range(total_records), min(dirty_count, total_records)):
        records[idx] = dirty_record(records[idx])

    # Inject corruption
    records = inject_bad_states(records)
    records = inject_donor_id_duplicates(records, donor_id_duplicate_count)
    records = inject_record_duplicates(records, record_duplicate_count)
    records = inject_email_duplicates(records, email_duplicate_count)  # global email duplicates

    random.shuffle(records)
    df = pd.DataFrame(records)
    return df

# =========================
# Save Panels
# =========================
def save_panel_csvs(df, output_dir, base_name="Acme_Corporation"):
    os.makedirs(output_dir, exist_ok=True)
    for panel in sorted(df["panel"].unique()):
        panel_df = df[df["panel"] == panel]
        file_path = os.path.join(output_dir, f"{base_name}_P{panel}.csv")
        panel_df.to_csv(file_path, index=False)
        print(f"✅ Panel {panel} saved: {file_path}")

# =========================
# Run Example
# =========================
if __name__ == "__main__":
    panel_sizes = {1: 12000, 2: 7000, 3: 30000}

    df = generate_large_dataset(
        panel_sizes=panel_sizes,
        dirty_count=40,
        donor_id_duplicate_count=10,
        email_duplicate_count=6,  # global duplicates
        record_duplicate_count=25
    )

    output_dir = os.path.join(os.getcwd(), "data", "Acme Corporation")
    save_panel_csvs(df, output_dir)

    print("🎯 Dataset generation completed.")
