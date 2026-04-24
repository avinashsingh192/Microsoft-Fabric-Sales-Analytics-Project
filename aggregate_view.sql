CREATE OR REPLACE TEMPORARY VIEW sale_by_date_employee
AS
SELECT 
    DD.Date,
    DD.CalendarMonthLabel,
    DD.Day,
    DD.ShortMonth AS Month,
    DD.CalendarYear AS Year,
    DE.Employee,
    DE.PreferredName,
    SUM(FS.TotalExcludingTax) AS SumOfTotalExcludingTax,
    SUM(FS.TaxAmount) AS SumOfTaxAmount,
    SUM(FS.TotalIncludingTax) AS SumOfTotalIncludingTax,
    SUM(FS.Profit) AS SumOfProfit
FROM medium1.fact_sale AS FS
INNER JOIN medium1.dimension_date AS DD ON FS.InvoiceDateKey = DD.Date
INNER JOIN medium1.dimension_employee AS DE ON FS.SalespersonKey = DE.EmployeeKey
GROUP BY
    DD.Date,
    DD.CalendarMonthLabel,
    DD.Day,
    DD.ShortMonth,
    DD.CalendarYear,
    DE.Employee,
    DE.PreferredName
ORDER BY
    DD.Date ASC,
    DE.PreferredName ASC
