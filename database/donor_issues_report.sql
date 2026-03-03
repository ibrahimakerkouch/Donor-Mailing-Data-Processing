WITH DonorIssuesCTE AS
(
    SELECT
		JP.Job_ID,
        JP.Panel_ID,
        JP.Donor_ID AS 'Donor ID',
        JP.FileName AS 'Source File Name',
        JP.Reason AS 'Issue / Reason',
        JP.FieldCategory AS 'Data Category',
        JP.FieldsAffected AS 'Affected Fields'
    FROM Customers C
    JOIN Jobs J ON C.Customer_ID = J.Customer_ID
    JOIN Jobs_Panels JP ON J.Job_ID = JP.Job_ID
)
-- Example of using the CTE
SELECT *
FROM DonorIssuesCTE
Where Job_ID = 1 AND Panel_ID = 1;
