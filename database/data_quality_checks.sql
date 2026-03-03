-- Check if AddressLine1 not null
select * from Donors where AddressLine1 = '' and JobID = 1
-- Check if City not null
select * from Donors where City = '' and JobID = 1
-- Check if State not null
select * from Donors where State = '' and JobID = 1
-- Check if ZIP5 not null
select * from Donors where ZIP5 = '' and JobID = 1
-- Check if DonationAmount not null and no-negative
select * from Donors where DonationAmount <= 0 and JobID = 1
-- Check if ZIP+4 not null
select * from Donors where ZIP4 = '' and JobID = 1
-- Check if DeliveryPoint not null
select * from Donors where DeliveryPoint = '' and JobID = 1
-- Check if BarcodeIdentifier not null
select * from Donors where BarcodeIdentifier = '' and JobID = 1
-- Check if ServiceTypeID not null
select * from Donors where ServiceTypeID = '' and JobID = 1
-- Check if MailerID not null
select * from Donors where MailerID = '' and JobID = 1
-- Check if SerialNumber not null
select * from Donors where SerialNumber = '' and JobID = 1

-- Check if the following columns Title, FirstName, LastName and Suffix are trimmed
select * from Donors where trim(Title) != Title and JobID = 1
select * from Donors where trim(FirstName) != FirstName and JobID = 1
select * from Donors where trim(LastName) != LastName and JobID = 1
select * from Donors where trim(Suffix) != Suffix and JobID = 1

-- Check if GreetingLine column is missing
select * from Donors where GreetingLine not like '%Dear%' and jobID = 1

-- Check if state codes are Validated
select * from Donors where jobID = 1 and  State not in ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA', 'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT', 'VA','WA','WV','WI','WY');

-- Check the lengths if they are correct
select * from Donors where len(BarcodeIdentifier) != 2 and jobID = 1
select * from Donors where len(ServiceTypeID) != 3 and jobID = 1
select * from Donors where len(MailerID) != 6 and jobID = 1
select * from Donors where len(SerialNumber) != 9 and jobID = 1
select * from Donors where len(ZIP5) != 5 and jobID = 1
select * from Donors where len(ZIP4) != 4 and jobID = 1
select * from Donors where len(DeliveryPoint) != 2 and jobID = 1

-- Check Demographics & Geography columns
select * from Donors where Representative = '' and jobID = 1
select * from Donors where Senator = '' and jobID = 1
select * from Donors where CongressionalDistrict = '' and jobID = 1
select * from Donors where CongressionalName = '' and jobID = 1

-- Check if email has invalid format
select * from Donors where Email not like '%@%.%' and jobID = 1

-- Check if phone number has invalid format
select * from Donors where Phone not like '(%)%-%' and jobID = 1

-- Check duplicates of DonorID, Phone number and Email
select DonorID, count(*) as 'Counts' from Donors where jobID = 3 group by DonorID having count(*) > 1
select Phone, count(*) as 'Counts' from Donors where jobID = 3 group by Phone having count(*) > 1
select Email, count(*) as 'Counts' from Donors where jobID = 3 group by Email having count(*) > 1
