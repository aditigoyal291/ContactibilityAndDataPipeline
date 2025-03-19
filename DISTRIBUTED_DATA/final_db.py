from faker import Faker
from pymongo import MongoClient
import random
from datetime import datetime, timedelta
import re

# Initialize Faker with Indian locale
fake = Faker('en_IN')

# Connect to MongoDB
client = MongoClient('mongodb://contact:contact1234@localhost:27017/?authSource=admin')
db = client['Plan5']  # Database name

# Clear existing collections to avoid duplicates
db.PrimaryApplicant.drop()
db.Company.drop()
db.Loan.drop()
db.Coapplicant.drop()
db.Reference.drop()
db.Person.drop()  # Add a new Person collection to store unique person data

# Create collections
companies = db.Company
persons = db.Person  # New collection for unique person data
primary_applicants = db.PrimaryApplicant
loans = db.Loan
coapplicants = db.Coapplicant
references = db.Reference

# Indian cities
indian_cities = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
    "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal",
    "Visakhapatnam", "Patna", "Vadodara", "Ghaziabad", "Ludhiana"
]

# Indian company sectors
indian_sectors = [
    "IT Services", "Pharmaceuticals", "Automotive", "Textiles",
    "Banking & Finance", "Energy", "Telecom", "FMCG",
    "Infrastructure", "Steel", "Agriculture", "Healthcare",
    "Education", "Real Estate", "E-commerce"
]


# Custom phone number generator for Indian format: +91 followed by a 10-digit number starting with 6-9
def generate_indian_phone():
    first_digit = random.choice(['6', '7', '8', '9'])
    rest_digits = ''.join([str(random.randint(0, 9)) for _ in range(9)])
    return f"+91 {first_digit}{rest_digits}"


# Custom PAN generator: 5 uppercase letters + 4 digits + 1 uppercase letter
def generate_pan():
    letters1 = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))
    digits = ''.join([str(random.randint(0, 9)) for _ in range(4)])
    letter2 = random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    return f"{letters1}{digits}{letter2}"


# Custom Aadhar generator: 12 digits
def generate_aadhar():
    return ''.join([str(random.randint(0, 9)) for _ in range(12)])


# Generate companies
company_ids = []
company_data = []
for i in range(10):
    # Generate custom company ID: E001, E002, etc.
    company_id = f"E{(i + 1):03d}"
    company_ids.append(company_id)

    city = random.choice(indian_cities)

    company_data.append({
        "companyId": company_id,
        "name": fake.company(),
        "sector": random.choice(indian_sectors),
        "address": f"{fake.building_number()}, {fake.street_name()}, {city} - {fake.postcode()}",
        "phone": generate_indian_phone(),
        "website": fake.domain_name(),
        "founded": fake.year(),
        "gst_number": f"27{fake.numerify('AAAAA####A')}1ZY"
    })
companies.insert_many(company_data)
print(f"Created {len(company_data)} companies")

# Generate unique person data
person_data = []
person_ids = []
person_pan_map = {}  # Map PAN to personId

for i in range(50):
    city = random.choice(indian_cities)

    # Generate core person details
    pan = generate_pan()
    aadhar = generate_aadhar()
    person_id = f"PERSON{i + 1:03d}"

    person = {
        "personId": person_id,
        "name": fake.name(),
        "email": fake.email(),
        "phone": generate_indian_phone(),
        "address": f"{fake.building_number()}, {fake.street_name()}, {city} - {fake.postcode()}",
        "dateOfBirth": fake.date_of_birth(minimum_age=21, maximum_age=65).strftime("%Y-%m-%d"),
        "pan": pan,
        "aadhar": aadhar,
        "income": random.randint(250000, 2500000),  # Annual income in rupees
        "companyId": random.choice(company_ids),
        "position": fake.job(),
        "yearsEmployed": random.randint(1, 15),
        "roles": {
            "primaryApplicant": [],  # Will store primaryApplicantIds
            "coApplicant": [],  # Will store coApplicantIds
            "reference": []  # Will store referenceIds
        }
    }

    person_data.append(person)
    person_ids.append(person_id)
    person_pan_map[pan] = person_id

persons.insert_many(person_data)
print(f"Created {len(person_data)} unique persons")

# Generate primary applicants
primary_applicant_ids = []
primary_applicant_data = []

# Use the first 30 people as primary applicants
for i in range(30):
    # Get the person data
    person_id = person_ids[i]
    person_info = persons.find_one({"personId": person_id})

    # Generate custom primary applicant ID: P001, P002, etc.
    primary_id = f"P{(i + 1):03d}"
    primary_applicant_ids.append(primary_id)

    # Create primary applicant record with reference to person
    primary_applicant = {
        "primaryApplicantId": primary_id,
        "personId": person_id,
        "name": person_info["name"],  # Include some basic info for queries
        "pan": person_info["pan"],  # Include PAN for queries
        "loans": [],  # Will be populated with loan IDs
        "isCoApplicantFor": [],  # Will track loans where they are co-applicants
        "isReferenceFor": []  # Will track loans where they are references
    }

    primary_applicant_data.append(primary_applicant)

    # Update the person record to include this role
    persons.update_one(
        {"personId": person_id},
        {"$push": {"roles.primaryApplicant": primary_id}}
    )

primary_applicants.insert_many(primary_applicant_data)
print(f"Created {len(primary_applicant_data)} primary applicants")

# Loan types and their ranges (in rupees)
loan_types = {
    "Personal": (100000, 1500000),
    "Home": (2000000, 10000000),
    "Car": (500000, 3000000),
    "Education": (500000, 4000000),
    "Business": (1000000, 15000000)
}

loan_statuses = ["Pending", "Approved", "Rejected", "In Review"]

# Generate initial loans - ensuring each primary applicant has 1-3 loans
loan_counter = 0
loan_data = []
loan_ids = []

# First, assign 1-3 loans to each primary applicant
for primary_id in primary_applicant_ids:
    # Decide how many loans this applicant will have (1-3)
    num_loans = random.randint(1, 3)

    primary_loans = []  # To track loans for this primary applicant

    for _ in range(num_loans):
        loan_counter += 1

        # Generate custom loan ID: L001, L002, etc.
        loan_id = f"L{loan_counter:03d}"
        loan_ids.append(loan_id)

        # Select random loan type and amount
        loan_type = random.choice(list(loan_types.keys()))
        min_amount, max_amount = loan_types[loan_type]
        loan_amount = random.randint(min_amount, max_amount)

        # Generate random dates
        application_date = fake.date_time_between(start_date="-1y", end_date="now")
        decision_date = application_date + timedelta(days=random.randint(5, 30))

        loan_data.append({
            "loanId": loan_id,
            "primaryApplicantId": primary_id,
            "loanType": loan_type,
            "amount": loan_amount,
            "term": random.choice([12, 24, 36, 48, 60, 120, 180, 240]),  # Months
            "interestRate": round(random.uniform(7.5, 14.0), 2),  # Indian interest rates
            "applicationDate": application_date,
            "decisionDate": decision_date,
            "status": random.choice(loan_statuses),
            "coapplicantId": None,  # Will be populated with co-applicant ID
            "references": []  # Will be populated with reference IDs
        })

        primary_loans.append(loan_id)

    # Update the primary applicant's loans array
    primary_applicants.update_one(
        {"primaryApplicantId": primary_id},
        {"$set": {"loans": primary_loans}}
    )

loans.insert_many(loan_data)
print(f"Created {len(loan_data)} initial loans")

# Generate coapplicants - exactly one for each loan
coapplicant_counter = 0
coapplicant_data = []

# Dictionary to track which person IDs are already used as co-applicants
for loan_id in loan_ids:
    coapplicant_counter += 1

    # Generate custom coapplicant ID: C001, C002, etc.
    coapplicant_id = f"C{coapplicant_counter:03d}"

    # Get the loan information
    loan_info = loans.find_one({"loanId": loan_id})
    primary_id = loan_info["primaryApplicantId"]
    primary_info = primary_applicants.find_one({"primaryApplicantId": primary_id})

    # Select a person who is not already the primary applicant for this loan
    # For better interconnection, sometimes use existing primary applicants as co-applicants
    if random.random() < 0.3 and len(primary_applicant_ids) > 1:
        # Select a primary applicant different from the one on this loan
        possible_primaries = [pid for pid in primary_applicant_ids if pid != primary_id]
        selected_primary_id = random.choice(possible_primaries)
        selected_primary = primary_applicants.find_one({"primaryApplicantId": selected_primary_id})
        selected_person_id = selected_primary["personId"]
        selected_person = persons.find_one({"personId": selected_person_id})

        # Add co-applicant relationship to the primary applicant
        primary_applicants.update_one(
            {"primaryApplicantId": selected_primary_id},
            {"$push": {"isCoApplicantFor": loan_id}}
        )
    else:
        # Use a person not yet used as a primary applicant (from indices 30-49)
        person_index = 30 + (coapplicant_counter % 20)  # This gives indices 30-49, cycling if needed
        selected_person_id = person_ids[person_index]
        selected_person = persons.find_one({"personId": selected_person_id})

    # Create the co-applicant record
    coapplicant = {
        "coApplicantId": coapplicant_id,
        "personId": selected_person_id,
        "loanId": loan_id,
        "name": selected_person["name"],  # For easy queries
        "pan": selected_person["pan"],  # For easy queries
        "relationToPrimary": random.choice(["Spouse", "Parent", "Child", "Sibling", "Friend", "Relative"])
    }

    coapplicant_data.append(coapplicant)

    # Update the person to include this co-applicant role
    persons.update_one(
        {"personId": selected_person_id},
        {"$push": {"roles.coApplicant": coapplicant_id}}
    )

    # Update the loan's coapplicant field
    loans.update_one(
        {"loanId": loan_id},
        {"$set": {"coapplicantId": coapplicant_id}}
    )

coapplicants.insert_many(coapplicant_data)
print(f"Created {len(coapplicant_data)} initial coapplicants")

# Generate references - 1-3 references per loan
reference_counter = 0
reference_data = []

for loan_id in loan_ids:
    # Decide how many references this loan will have (1-3)
    num_references = random.randint(1, 3)
    loan_references = []  # To track references for this loan

    # Get loan information
    loan_info = loans.find_one({"loanId": loan_id})
    primary_id = loan_info["primaryApplicantId"]
    coapp_id = loan_info["coapplicantId"]

    # Get the person IDs associated with primary and co-applicant
    primary_info = primary_applicants.find_one({"primaryApplicantId": primary_id})
    primary_person_id = primary_info["personId"]

    coapp_info = coapplicants.find_one({"coApplicantId": coapp_id})
    coapp_person_id = coapp_info["personId"]

    for j in range(num_references):
        reference_counter += 1

        # Generate custom reference ID: R001, R002, etc.
        reference_id = f"R{reference_counter:03d}"

        # For references, let's mix in both primary applicants and co-applicants from other loans
        if random.random() < 0.6:
            # 60% chance to use a primary applicant as reference (not from this loan)
            possible_primary_ids = [pid for pid in primary_applicant_ids if pid != primary_id]

            if possible_primary_ids:
                selected_primary_id = random.choice(possible_primary_ids)
                selected_primary = primary_applicants.find_one({"primaryApplicantId": selected_primary_id})
                selected_person_id = selected_primary["personId"]
                selected_person = persons.find_one({"personId": selected_person_id})

                # Update the primary applicant to show they are a reference
                primary_applicants.update_one(
                    {"primaryApplicantId": selected_primary_id},
                    {"$push": {"isReferenceFor": loan_id}}
                )

                relation_link = f"primary:{selected_primary_id}"
            else:
                # If no suitable primary applicant, use a new person
                person_index = (30 + reference_counter) % len(person_ids)  # Cycle through the pool
                selected_person_id = person_ids[person_index]
                selected_person = persons.find_one({"personId": selected_person_id})
                relation_link = None
        elif random.random() < 0.4:
            # Use a co-applicant from another loan as reference
            possible_coapps = coapplicants.find({"coApplicantId": {"$ne": coapp_id}})
            possible_coapp_list = list(possible_coapps)

            if possible_coapp_list:
                selected_coapp = random.choice(possible_coapp_list)
                selected_person_id = selected_coapp["personId"]
                selected_person = persons.find_one({"personId": selected_person_id})
                relation_link = f"coapplicant:{selected_coapp['coApplicantId']}"
            else:
                # If no suitable co-applicant, use a new person
                person_index = (30 + reference_counter) % len(person_ids)  # Cycle through the pool
                selected_person_id = person_ids[person_index]
                selected_person = persons.find_one({"personId": selected_person_id})
                relation_link = None
        else:
            # Use a person not yet used in any role
            person_index = (30 + reference_counter) % len(person_ids)  # Cycle through the pool
            selected_person_id = person_ids[person_index]
            selected_person = persons.find_one({"personId": selected_person_id})
            relation_link = None

        # Ensure we don't use the primary applicant or co-applicant from this loan
        if selected_person_id == primary_person_id or selected_person_id == coapp_person_id:
            # Try using a different person
            unused_person_ids = [pid for pid in person_ids
                                 if pid != primary_person_id and pid != coapp_person_id]
            if unused_person_ids:
                selected_person_id = random.choice(unused_person_ids)
                selected_person = persons.find_one({"personId": selected_person_id})
                relation_link = None
            else:
                # Skip this reference if no suitable person is found
                continue

        # Create the reference record
        reference = {
            "referenceId": reference_id,
            "personId": selected_person_id,
            "name": selected_person["name"],  # For easy queries
            "pan": selected_person["pan"],  # For easy queries
            "loanId": loan_id,
            "relationToBorrower": random.choice([
                "Family", "Friend", "Colleague", "Employer", "Neighbor", "Relative"
            ]),
            "yearsKnown": random.randint(1, 30)
        }

        # Add the relation link if applicable
        if relation_link:
            reference["linkedTo"] = relation_link

        reference_data.append(reference)
        loan_references.append(reference_id)

        # Update the person to include this reference role
        persons.update_one(
            {"personId": selected_person_id},
            {"$push": {"roles.reference": reference_id}}
        )

    # Update the loan's references array
    loans.update_one(
        {"loanId": loan_id},
        {"$set": {"references": loan_references}}
    )

references.insert_many(reference_data)
print(f"Created {len(reference_data)} initial references")

# Now, let's make references also have their own loans as primary applicants
# We'll select some references and create new loans for them as primary applicants
additional_loan_counter = loan_counter
additional_loan_data = []
references_as_primary = []

# Use 40% of references as primary applicants
reference_list = list(references.find({"linkedTo": {"$exists": False}}))
num_references_to_make_primary = int(len(reference_list) * 0.4)
reference_selections = random.sample(reference_list, num_references_to_make_primary)

for reference_info in reference_selections:
    reference_id = reference_info["referenceId"]
    person_id = reference_info["personId"]
    person_info = persons.find_one({"personId": person_id})

    # Create a new primary applicant ID for this reference
    new_primary_id = f"P{len(primary_applicant_ids) + len(references_as_primary) + 1:03d}"

    # Create the primary applicant record
    primary_data = {
        "primaryApplicantId": new_primary_id,
        "personId": person_id,
        "name": person_info["name"],
        "pan": person_info["pan"],
        "loans": [],
        "isReferenceFor": [reference_info["loanId"]],  # They are already a reference
        "isCoApplicantFor": []
    }

    # Create 1-2 loans for this primary applicant
    num_loans = random.randint(1, 2)
    primary_loans = []

    for _ in range(num_loans):
        additional_loan_counter += 1
        new_loan_id = f"L{additional_loan_counter:03d}"

        # Select random loan type and amount
        loan_type = random.choice(list(loan_types.keys()))
        min_amount, max_amount = loan_types[loan_type]
        loan_amount = random.randint(min_amount, max_amount)

        # Generate random dates
        application_date = fake.date_time_between(start_date="-1y", end_date="now")
        decision_date = application_date + timedelta(days=random.randint(5, 30))

        # Create the loan
        loan_data = {
            "loanId": new_loan_id,
            "primaryApplicantId": new_primary_id,
            "loanType": loan_type,
            "amount": loan_amount,
            "term": random.choice([12, 24, 36, 48, 60, 120, 180, 240]),
            "interestRate": round(random.uniform(7.5, 14.0), 2),
            "applicationDate": application_date,
            "decisionDate": decision_date,
            "status": random.choice(loan_statuses),
            "coapplicantId": None,
            "references": []
        }

        additional_loan_data.append(loan_data)
        primary_loans.append(new_loan_id)
        loan_ids.append(new_loan_id)

    # Update primary loans
    primary_data["loans"] = primary_loans
    references_as_primary.append(primary_data)
    primary_applicant_ids.append(new_primary_id)

    # Update the person record to include this primary applicant role
    persons.update_one(
        {"personId": person_id},
        {"$push": {"roles.primaryApplicant": new_primary_id}}
    )

# Insert the additional primary applicants (former references)
if references_as_primary:
    primary_applicants.insert_many(references_as_primary)
    print(f"Created {len(references_as_primary)} additional primary applicants from references")

# Let's make co-applicants own loans as Primary Applicants
coapp_as_primary = []

# Use 30% of co-applicants as primary applicants
coapp_list = list(coapplicants.find())
num_coapps_to_make_primary = int(len(coapp_list) * 0.3)
coapp_selections = random.sample(coapp_list, num_coapps_to_make_primary)

for coapp_info in coapp_selections:
    coapp_id = coapp_info["coApplicantId"]
    person_id = coapp_info["personId"]
    person_info = persons.find_one({"personId": person_id})

    # Create a new primary applicant ID for this co-applicant
    new_primary_id = f"P{len(primary_applicant_ids) + 1:03d}"
    primary_applicant_ids.append(new_primary_id)

    # Create the primary applicant record
    primary_data = {
        "primaryApplicantId": new_primary_id,
        "personId": person_id,
        "name": person_info["name"],
        "pan": person_info["pan"],
        "loans": [],
        "isReferenceFor": [],
        "isCoApplicantFor": [coapp_info["loanId"]]  # They are already a co-applicant
    }

    # Create 1-2 loans for this primary applicant
    num_loans = random.randint(1, 2)
    primary_loans = []

    for _ in range(num_loans):
        additional_loan_counter += 1
        new_loan_id = f"L{additional_loan_counter:03d}"

        # Select random loan type and amount
        loan_type = random.choice(list(loan_types.keys()))
        min_amount, max_amount = loan_types[loan_type]
        loan_amount = random.randint(min_amount, max_amount)

        # Generate random dates
        application_date = fake.date_time_between(start_date="-1y", end_date="now")
        decision_date = application_date + timedelta(days=random.randint(5, 30))

        # Create the loan
        loan_data = {
            "loanId": new_loan_id,
            "primaryApplicantId": new_primary_id,
            "loanType": loan_type,
            "amount": loan_amount,
            "term": random.choice([12, 24, 36, 48, 60, 120, 180, 240]),
            "interestRate": round(random.uniform(7.5, 14.0), 2),
            "applicationDate": application_date,
            "decisionDate": decision_date,
            "status": random.choice(loan_statuses),
            "coapplicantId": None,
            "references": []
        }

        additional_loan_data.append(loan_data)
        primary_loans.append(new_loan_id)
        loan_ids.append(new_loan_id)

    # Update primary loans
    primary_data["loans"] = primary_loans
    coapp_as_primary.append(primary_data)

    # Update the person record to include this primary applicant role
    persons.update_one(
        {"personId": person_id},
        {"$push": {"roles.primaryApplicant": new_primary_id}}
    )

# Insert the additional primary applicants (former co-applicants)
if coapp_as_primary:
    primary_applicants.insert_many(coapp_as_primary)
    print(f"Created {len(coapp_as_primary)} additional primary applicants from co-applicants")

# Insert all the additional loans
if additional_loan_data:
    loans.insert_many(additional_loan_data)
    print(f"Created {len(additional_loan_data)} additional loans")

# Now let's make primary applicants be co-applicants for some other loans
additional_coapplicant_data = []

# For each additional loan, select a co-applicant from existing primary applicants
for loan_id in [l["loanId"] for l in additional_loan_data]:
    coapplicant_counter += 1
    new_coapplicant_id = f"C{coapplicant_counter:03d}"

    # Get the loan info
    loan_info = loans.find_one({"loanId": loan_id})
    current_primary_id = loan_info["primaryApplicantId"]

    # Get the person associated with the primary applicant
    primary_info = primary_applicants.find_one({"primaryApplicantId": current_primary_id})
    current_person_id = primary_info["personId"]

    # Select a primary applicant different from the loan owner
    potential_primaries = [pid for pid in primary_applicant_ids if pid != current_primary_id]
    if not potential_primaries:
        continue

    selected_primary_id = random.choice(potential_primaries)
    primary_info = primary_applicants.find_one({"primaryApplicantId": selected_primary_id})
    selected_person_id = primary_info["personId"]
    person_info = persons.find_one({"personId": selected_person_id})

    # Create co-applicant data
    coapp_data = {
        "coApplicantId": new_coapplicant_id,
        "personId": selected_person_id,
        "name": person_info["name"],
        "pan": person_info["pan"],
        "loanId": loan_id,
        "relationToPrimary": random.choice(["Spouse", "Parent", "Child", "Sibling", "Friend", "Relative"]),
        "linkedTo": f"primary:{selected_primary_id}"
    }

    additional_coapplicant_data.append(coapp_data)

    # Update the loan with this co-applicant
    loans.update_one(
        {"loanId": loan_id},
        {"$set": {"coapplicantId": new_coapplicant_id}}
    )

    # Update the primary applicant to show they are a co-applicant for this loan
    primary_applicants.update_one(
        {"primaryApplicantId": selected_primary_id},
        {"$push": {"isCoApplicantFor": loan_id}}
    )

    # Update the person record to include this co-applicant role
    persons.update_one(
        {"personId": selected_person_id},
        {"$push": {"roles.coApplicant": new_coapplicant_id}}
    )

# Insert additional co-applicants
if additional_coapplicant_data:
    coapplicants.insert_many(additional_coapplicant_data)
    print(f"Created {len(additional_coapplicant_data)} additional co-applicants from primary applicants")

# Finally, let's add references to the additional loans
additional_reference_data = []
reference_counter_start = reference_counter

# For each additional loan, add 1-3 references
for loan_id in [l["loanId"] for l in additional_loan_data]:
    # Decide how many references this loan will have
    num_references = random.randint(1, 3)
    loan_references = []

    # Get loan information
    loan_info = loans.find_one({"loanId": loan_id})
    primary_id = loan_info["primaryApplicantId"]
    coapp_id = loan_info["coapplicantId"] if loan_info["coapplicantId"] else None

    # Get the person IDs associated with primary and co-applicant
    primary_info = primary_applicants.find_one({"primaryApplicantId": primary_id})
    primary_person_id = primary_info["personId"]

    coapp_person_id = None
    if coapp_id:
        coapp_info = coapplicants.find_one({"coApplicantId": coapp_id})
        if coapp_info:
            coapp_person_id = coapp_info["personId"]

    for j in range(num_references):
        reference_counter += 1
        new_reference_id = f"R{reference_counter:03d}"

        # Let's sometimes use existing primary applicants as references (50% chance)
        if random.random() < 0.5:
            # Make sure we don't use the primary applicant or co-applicant of this loan
            potential_primaries = [
                pid for pid in primary_applicant_ids
                if pid != primary_id
            ]

            if potential_primaries:
                selected_primary_id = random.choice(potential_primaries)
                primary_info = primary_applicants.find_one({"primaryApplicantId": selected_primary_id})
                selected_person_id = primary_info["personId"]

                # Ensure we don't use the same person that's already involved in
                # Ensure we don't use the same person that's already involved in this loan
                if selected_person_id == primary_person_id or (
                        coapp_person_id and selected_person_id == coapp_person_id):
                    # Try a different approach - use a person not yet associated
                    person_index = (30 + reference_counter) % len(person_ids)
                    selected_person_id = person_ids[person_index]
                    selected_person = persons.find_one({"personId": selected_person_id})
                    relation_link = None
                else:
                    # Use the primary applicant as reference
                    person_info = persons.find_one({"personId": selected_person_id})
                    relation_link = f"primary:{selected_primary_id}"

                    # Update the primary applicant to show they are a reference
                    primary_applicants.update_one(
                        {"primaryApplicantId": selected_primary_id},
                        {"$push": {"isReferenceFor": loan_id}}
                    )
            else:
                # Use a person not yet associated with the loan
                person_index = (30 + reference_counter) % len(person_ids)
                selected_person_id = person_ids[person_index]
                selected_person = persons.find_one({"personId": selected_person_id})
                relation_link = None
        else:
            # Use a co-applicant from another loan (25% chance)
            if random.random() < 0.5 and coapp_person_id:
                possible_coapps = list(coapplicants.find({
                    "personId": {"$ne": primary_person_id},
                    "personId": {"$ne": coapp_person_id}
                }))

                if possible_coapps:
                    selected_coapp = random.choice(possible_coapps)
                    selected_person_id = selected_coapp["personId"]
                    selected_person = persons.find_one({"personId": selected_person_id})
                    relation_link = f"coapplicant:{selected_coapp['coApplicantId']}"
                else:
                    # Use a person not yet associated with the loan
                    person_index = (30 + reference_counter) % len(person_ids)
                    selected_person_id = person_ids[person_index]
                    selected_person = persons.find_one({"personId": selected_person_id})
                    relation_link = None
            else:
                # Use a person not yet associated with the loan
                person_index = (30 + reference_counter) % len(person_ids)
                selected_person_id = person_ids[person_index]
                selected_person = persons.find_one({"personId": selected_person_id})
                relation_link = None

            # Create the reference record
        reference = {
            "referenceId": new_reference_id,
            "personId": selected_person_id,
            "name": selected_person["name"],
            "pan": selected_person["pan"],
            "loanId": loan_id,
            "relationToBorrower": random.choice([
                "Family", "Friend", "Colleague", "Employer", "Neighbor", "Relative"
            ]),
            "yearsKnown": random.randint(1, 30)
        }

        # Add the relation link if applicable
        if relation_link:
            reference["linkedTo"] = relation_link

        additional_reference_data.append(reference)
        loan_references.append(new_reference_id)

        # Update the person to include this reference role
        persons.update_one(
            {"personId": selected_person_id},
            {"$push": {"roles.reference": new_reference_id}}
        )

    # Update the loan's references array
    loans.update_one(
        {"loanId": loan_id},
        {"$set": {"references": loan_references}}
    )

# Insert additional references
if additional_reference_data:
    references.insert_many(additional_reference_data)
    print(f"Created {len(additional_reference_data)} additional references")

# Create some indexes for faster queries
primary_applicants.create_index("personId")
primary_applicants.create_index("pan")
coapplicants.create_index("personId")
coapplicants.create_index("loanId")
references.create_index("personId")
references.create_index("loanId")
persons.create_index("pan", unique=True)
persons.create_index("personId", unique=True)
loans.create_index("primaryApplicantId")
loans.create_index("coapplicantId")

print("Created indexes for faster queries")

# Print some summary statistics
print("\nSummary Statistics:")
print(f"Total Persons: {persons.count_documents({})}")
print(f"Total Companies: {companies.count_documents({})}")
print(f"Total Primary Applicants: {primary_applicants.count_documents({})}")
print(f"Total Loans: {loans.count_documents({})}")
print(f"Total Co-applicants: {coapplicants.count_documents({})}")
print(f"Total References: {references.count_documents({})}")

print("\nData generation complete!")