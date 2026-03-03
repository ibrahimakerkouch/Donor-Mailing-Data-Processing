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

def maybe_short(value, expected_len, chance=0.15):
    value_str = str(value)
    if random.random() < chance and len(value_str) > 1:
        return value_str[:random.randint(1, expected_len-1)]
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

    zip5 = generate_number(5)
    zip4 = generate_number(4)
    deliveryPoint = generate_number(2)
    barcodeIdentifier = generate_number(2)
    serviceTypeId = generate_number(3)
    mailerId = generate_number(6)
    serialNumber = generate_number(9)

    addressLine1 = fake.street_address()
    addressLine2 = random.choice(["Apt 2B", "Suite 300", "", "Floor 5"])
    if addressLine1 == "":
        addressLine2 = ""

    return {
        "panel": panel,
        "donorId": donor_id,
        "title": title,
        "firstName": first,
        "lastName": last,
        "suffix": random.choice(["", "Jr.", "Sr.", "III"]),
        "greetingLine": f"Dear {title} {last}",
        "company": fake.company(),
        "department": random.choice(["HR", "Finance", "IT", "Research", ""]),
        "addressLine1": addressLine1,
        "addressLine2": addressLine2,
        "city": fake.city(),
        "state": random.choice(US_STATES),
        "zip5": zip5,
        "zip4": zip4,
        "email": f"{first}.{last}{email_suffix}@example.com",
        "phone": fake.phone_number(),
        "fax": fake.phone_number(),
        "donationAmount": round(random.uniform(5, 500), 2),
        "mailCode": random.choice(["A1", "B2", "C3", "D4"]),
        "deliveryPoint": deliveryPoint,
        "barcodeIdentifier": barcodeIdentifier,
        "serviceTypeId": serviceTypeId,
        "mailerId": mailerId,
        "serialNumber": serialNumber,
        "congressionalDistrict": str(random.randint(1, 12)),
        "congressionalName": f"{fake.state()}-{random.randint(1,12)}",
        "representative": fake.name(),
        "senator": fake.name()
    }

# ======== Dirty Record ========
def dirty_record(record, null_chance=0.1, short_chance=0.15):
    expected_lengths = {
        "zip5": 5,
        "zip4": 4,
        "deliveryPoint": 2,
        "barcodeIdentifier": 2,
        "serviceTypeId": 3,
        "mailerId": 6,
        "serialNumber": 9
    }
    record = record.copy()
    for col, val in record.items():
        if col not in {"panel", "donorId"}:
            record[col] = maybe_null(val, chance=null_chance)
        if col in expected_lengths and record[col] != "":
            record[col] = maybe_short(record[col], expected_lengths[col], chance=short_chance)
    if record["addressLine1"] == "":
        record["addressLine2"] = ""
    return record

# ======== Corruption Injectors ========
def inject_donor_id_duplicates(records, duplicate_count):
    donor_ids = [r["donorId"] for r in records]
    for idx in random.sample(range(len(records)), duplicate_count):
        records[idx]["donorId"] = random.choice(donor_ids)
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

def inject_record_duplicates(records, duplicate_count):
    for idx in random.sample(range(len(records)), duplicate_count):
        records[idx] = random.choice(records).copy()
    return records

def inject_bad_states(records):
    for idx in random.sample(range(len(records)), random.randint(1, 4)):
        records[idx]["state"] = random.choice(BAD_STATES)
    return records

# ======== Dataset Generator ========
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

    # Inject corruption
    records = inject_bad_states(records)
    records = inject_donor_id_duplicates(records, donor_id_duplicate_count)
    records = inject_record_duplicates(records, record_duplicate_count)
    records = inject_email_duplicates(records, email_duplicate_count)  # global duplicates

    random.shuffle(records)
    df = pd.DataFrame(records)
    return df

# ======== Save Panels ========
def save_panels(df, output_dir, base_name="Beta_Industries"):
    os.makedirs(output_dir, exist_ok=True)
    for panel in sorted(df["panel"].unique()):
        panel_df = df[df["panel"] == panel]
        file_path = os.path.join(output_dir, f"{base_name}_P{panel}.csv")
        panel_df.to_csv(file_path, index=False)
        print(f"✅ Panel {panel} saved: {file_path}")

# ======== Run Example ========
if __name__ == "__main__":
    panel_sizes = {
        1: 20000,
        2: 10000,
        3: 6000,
        4: 13000
    }

    df = generate_dataset(
        panel_sizes,
        dirty_count=110,
        donor_id_duplicate_count=13,
        email_duplicate_count=9,  # global duplicates
        record_duplicate_count=40
    )

    output_dir = os.path.join(os.getcwd(), "data", "Beta Industries")
    save_panels(df, output_dir, base_name="Beta_Industries")

    print("🎯 Dataset generation completed.")
