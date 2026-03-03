USE master
go
-- Drop the database if it exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'DonorMailingDataProcessing')
BEGIN
    ALTER DATABASE DonorMailingDataProcessing SET SINGLE_USER WITH ROLLBACK IMMEDIATE;  -- Disconnect all users
    DROP DATABASE DonorMailingDataProcessing;
END
GO

-- Create the database
CREATE DATABASE DonorMailingDataProcessing;
GO

-- Optional: Use the new database
USE DonorMailingDataProcessing;
GO

CREATE TABLE Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,  	-- Unique identifier for each customer
    CustomerName VARCHAR(150) NOT NULL,         -- Customer name
    CreatedAt DATETIME DEFAULT GETDATE()        -- Record creation timestamp
);

CREATE TABLE Jobs (
    JobID INT PRIMARY KEY,  				-- Unique job identifier
	JobName VARCHAR(60) NOT NULL,  			-- Job name
	MailDate DATE NOT NULL,                	-- Scheduled mail date
    CustomerID INT NOT NULL,               	-- Customer owning the job
    CreatedAt DATETIME DEFAULT GETDATE(),   -- Job creation timestamp

    -- Relationship: each job belongs to one customer
    CONSTRAINT FKJobsCustomers
        FOREIGN KEY (CustomerID)
        REFERENCES Customers(CustomerID)
);


CREATE TABLE Panels (
    PanelID INT IDENTITY(1,1) PRIMARY KEY, -- Unique panel identifier
    PanelNumber VARCHAR(50) NOT NULL,      -- Business-facing panel number
    CreatedAt DATETIME DEFAULT GETDATE(),   -- Panel creation timestamp

    -- Prevent duplicate panel numbers
    CONSTRAINT UQPanelNumber UNIQUE (PanelNumber)
);

CREATE TABLE JobsPanels (
    JobID INT NOT NULL,                        	-- Related job
    PanelID INT NOT NULL,                      	-- Related panel
    DonorID INT NOT NULL,              			-- Donor identifier
    FileName VARCHAR(255) NOT NULL,             -- Source file name
    Stage VARCHAR(50) NOT NULL,                 -- ETL stage (Extract, Transform, Validate, BCC Upload)
    ActionTaken VARCHAR(100) NOT NULL,          -- Inserted, Updated, Skipped, Rejected
    Reason VARCHAR(255) NULL,                   -- Reason for action taken
    FieldCategory VARCHAR(50) NULL,         	-- Contact Info / Postal / Demographics
    FieldsAffected VARCHAR(255) NULL,           -- Fields impacted by the action
    InsertedAt DATETIME DEFAULT GETDATE(),      -- Processing timestamp

    -- Relationship: link to Jobs table
    CONSTRAINT FKJobsPanelsJobs
        FOREIGN KEY (JobID)
        REFERENCES Jobs(JobID),

    -- Relationship: link to Panels table
    CONSTRAINT FKJobsPanelsPanels
        FOREIGN KEY (PanelID)
        REFERENCES Panels(PanelID)
);

CREATE TABLE Donors (
    JobID INT NOT NULL,                          -- Foreign key to Jobs table
    PanelID INT NOT NULL,                        -- Foreign key to Panels table
    DonorID VARCHAR(50) NOT NULL,                -- Unique donor identifier
    Title VARCHAR(10) NULL,                      -- Name title (Mr, Ms, Dr, etc.)
    FirstName VARCHAR(50) NOT NULL,              -- Donor first name
    LastName VARCHAR(50) NOT NULL,               -- Donor last name
    Suffix VARCHAR(10) NULL,                     -- Name suffix (Jr, Sr, III)
    GreetingLine VARCHAR(100) NULL,              -- Personalized greeting line
    Company VARCHAR(100) NULL,                   -- Company name
    Department VARCHAR(50) NULL,                 -- Department name
    AddressLine1 VARCHAR(100) NOT NULL,          -- Primary address line
    AddressLine2 VARCHAR(100) NULL,              -- Secondary address line
    City VARCHAR(50) NOT NULL,                   -- City name
    State CHAR(2) NOT NULL,                      -- State abbreviation
    ZIP5 CHAR(5) NOT NULL,                       -- 5-digit ZIP code
    ZIP4 CHAR(4) NULL,                           -- ZIP+4 extension
    Email VARCHAR(100) NULL,                     -- Email address
    Phone VARCHAR(20) NULL,                      -- Phone number
    Fax VARCHAR(20) NULL,                        -- Recipient fax number
    DonationAmount DECIMAL(12, 2) NULL,          -- Donation amount
    MailCode VARCHAR(50) NULL,                   -- Internal mail code
    DeliveryPoint CHAR(2) NULL,                  -- USPS delivery point
    BarcodeIdentifier CHAR(2) NULL,              -- Mail barcode identifier
    ServiceTypeID CHAR(3) NULL,                  -- Mailing service type
    MailerID CHAR(6) NULL,                       -- Mailer identifier
    SerialNumber CHAR(9) NULL,                   -- Piece-level serial number
    CongressionalDistrict CHAR(5) NULL,          -- Congressional district
    Representative VARCHAR(100) NULL,            -- House representative
    Senator VARCHAR(100) NULL,                   -- Senator(s)
    CongressionalName VARCHAR(150) NULL,         -- Full congressional name

    CONSTRAINT FKDonorMailingInfoJobs FOREIGN KEY (JobID) REFERENCES Jobs(JobID),
    CONSTRAINT FKDonorMailingInfoPanels FOREIGN KEY (PanelID) REFERENCES Panels(PanelID)
);

CREATE TABLE AggregationLog (
    AggrID INT IDENTITY(1,1) PRIMARY KEY,  	-- Aggregation record identifier
    CustomerID INT NOT NULL,               	-- Customer associated with aggregation
    JobID INT NOT NULL,                    	-- Job being aggregated
    PanelID INT NOT NULL,                  	-- Panel being aggregated
    FileName VARCHAR(255) NOT NULL,         -- Source file name
    Stage VARCHAR(50) NOT NULL,             -- Processing stage
    ActionTaken VARCHAR(100) NOT NULL,      -- Aggregated, Failed, Partial
    Reason VARCHAR(255) NULL,               -- Failure or exception reason
	FieldCategory VARCHAR(50) NULL,         -- Contact Info / Postal / Demographics
    FieldsAffected VARCHAR(255) NULL,       -- Fields impacted during aggregation
    RecordCount INT NOT NULL,               -- Number of records processed
    AggregatedAt DATETIME DEFAULT GETDATE(),-- Aggregation timestamp

    -- Relationship: link to Customers
    CONSTRAINT FKAggrCustomers
        FOREIGN KEY (CustomerID)
        REFERENCES Customers(CustomerID),

    -- Relationship: link to Jobs
    CONSTRAINT FKAggrJobs
        FOREIGN KEY (JobID)
        REFERENCES Jobs(JobID),

    -- Relationship: link to Panels
    CONSTRAINT FKAggrPanels
        FOREIGN KEY (PanelID)
        REFERENCES Panels(PanelID)
);


-- Insert sample customers for testing purposes
INSERT INTO Customers (CustomerName)
VALUES 
('Acme Corporation'),  -- Customer 1
('Beta Industries'),   -- Customer 2
('Gamma Solutions'),   -- Customer 3
('Delta Enterprises');   -- Customer 4

-- Insert sample panels for testing purposes
INSERT INTO Panels (PanelNumber)
VALUES 
('01'),  -- Panel 1
('02'),  -- Panel 2
('03'),  -- Panel 3
('04'),  -- Panel 4
('05'),  -- Panel 5
('06'),  -- Panel 6
('07'),  -- Panel 7
('08'),  -- Panel 8
('09'),  -- Panel 9
('10');  -- Panel 10
