import random
import pandas as pd
from faker import Faker
import os

fake = Faker()

# ======== USPS / Bad States ========
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

# ======== DonorID ========
def generate_donor_id(min_len=5, max_len=8):
    length = random.randint(min_len, max_len)
    return "".join(str(random.randint(1, 9)) for _ in range(length))

def generate_unique_donor_ids(n):
    ids = set()
    while len(ids) < n:
        ids.add(generate_donor_id())
    return list(ids)

# ======== Clean Record ========
def generate_clean_record(panel, donor_id, email_suffix):
    first = fake.first_name()
    last = fake.last_name()
    title = random.choice(["Mr.", "Ms.", "Mrs.", "Dr."])

    address1 = fake.street_address()
    address2 = random.choice(["Apt 2B", "Suite 300", "", "Floor 5"])
    if address1 == "":
        address2 = ""

    return {
        "Panel": panel,
        "DonorID": donor_id,
        "Title": title,
        "FirstName": first,
        "LastName": last,
        "Suffix": random.choice(["", "Jr.", "Sr.", "III"]),
        "GreetingLine": f"Dear {title} {last}",
        "Company": fake.company(),
        "Department": random.choice(["HR", "Finance", "IT", "Research", ""]),
        "AddressLine1": address1,
        "AddressLine2": address2,
        "City": fake.city(),
        "State": random.choice(US_STATES),
        "ZIP5": "".join(str(random.randint(0,9)) for _ in range(5)),
        "ZIP4": "".join(str(random.randint(0,9)) for _ in range(4)),
        "Email": f"{first}.{last}{email_suffix}@example.com",
        "Phone": fake.phone_number(),
        "Fax": fake.phone_number(),
        "DonationAmount": round(random.uniform(5, 500), 2),
        "MailCode": random.choice(["A1", "B2", "C3", "D4"]),
        "DeliveryPoint": "".join(str(random.randint(0,9)) for _ in range(2)),
        "BarcodeIdentifier": "".join(str(random.randint(0,9)) for _ in range(2)),
        "ServiceTypeID": "".join(str(random.randint(0,9)) for _ in range(3)),
        "MailerID": "".join(str(random.randint(0,9)) for _ in range(6)),
        "SerialNumber": "".join(str(random.randint(0,9)) for _ in range(9)),
        "CongressionalDistrict": str(random.randint(1,12)),
        "CongressionalName": f"{fake.state()}-{random.randint(1,12)}",
        "Representative": fake.name(),
        "Senator": fake.name()
    }

# ======== Dirty Record ========
def dirty_record(record, null_chance=0.1, short_chance=0.15):
    numeric_fields = [
        "ZIP5","ZIP4","DeliveryPoint","BarcodeIdentifier",
        "ServiceTypeID","MailerID","SerialNumber"
    ]
    record = record.copy()
    for col, val in record.items():
        if col not in {"Panel","DonorID"}:
            record[col] = maybe_null(val, chance=null_chance)
        if col in numeric_fields and record[col] != "":
            record[col] = maybe_short(record[col], chance=short_chance)
    if record["AddressLine1"] == "":
        record["AddressLine2"] = ""
    return record

# ======== Inject Duplicates & Bad States ========
def inject_record_duplicates(records, record_duplicate_count):
    for idx in random.sample(range(len(records)), record_duplicate_count):
        records[idx] = random.choice(records).copy()
    return records

def inject_donorid_duplicates(records, donorid_duplicate_count):
    donor_ids = [r["DonorID"] for r in records]
    for idx in random.sample(range(len(records)), donorid_duplicate_count):
        records[idx]["DonorID"] = random.choice(donor_ids)
    return records

def inject_email_duplicates(records, email_duplicate_count):
    if email_duplicate_count == 0:
        return records
    indices = random.sample(range(len(records)), email_duplicate_count*2)
    sources = indices[:email_duplicate_count]
    targets = indices[email_duplicate_count:]
    for src_idx, tgt_idx in zip(sources, targets):
        records[tgt_idx]["Email"] = records[src_idx]["Email"]
    return records

def inject_bad_states(records):
    for idx in random.sample(range(len(records)), random.randint(1,4)):
        records[idx]["State"] = random.choice(BAD_STATES)
    return records

# ======== Main Dataset Generator ========
def generate_dataset(panel_sizes, dirty_count=0, record_duplicate_count=0,
                     donorid_duplicate_count=0, email_duplicate_count=0):
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
    records = inject_donorid_duplicates(records, donorid_duplicate_count)
    records = inject_record_duplicates(records, record_duplicate_count)
    records = inject_email_duplicates(records, email_duplicate_count)  # global duplicates

    random.shuffle(records)
    df = pd.DataFrame(records)
    return df

# ======== Split Panels ========
def split_panels(df):
    return {panel: df[df["Panel"]==panel].copy() for panel in df["Panel"].unique()}

# ======== Save Panels ========
def save_panels(panels, output_dir, base_name="Gamma_Solutions"):
    os.makedirs(output_dir, exist_ok=True)
    for panel, panel_df in panels.items():
        file_path = os.path.join(output_dir, f"{base_name}_P{panel}.csv")
        panel_df.to_csv(file_path, index=False)
        print(f"✅ Panel {panel} saved: {file_path}")

# ======== Run Example ========
if __name__ == "__main__":
    panel_sizes = {1: 15000, 2: 8000, 3: 9000, 4: 6000, 5: 10000}

    df = generate_dataset(
        panel_sizes,
        dirty_count=130,
        record_duplicate_count=35,
        donorid_duplicate_count=5,
        email_duplicate_count=2  # global email duplicates
    )

    panels = split_panels(df)
    output_dir = os.path.join(os.getcwd(), "data", "Gamma Solutions")
    save_panels(panels, output_dir, base_name="Gamma_Solutions")

    print("🎯 Large-scale dataset divided and saved by panels.")
