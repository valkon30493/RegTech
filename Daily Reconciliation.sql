/*Declare the weekday number*/
SET DATEFIRST 1;
DECLARE @ReportDay INT = DATEPART(dw,GETDATE());

/*Declare two report dates*/
--In case the report day is 0 (Monday) then the first report date is three days before today, in other words, Friday.
--Else the first report date is one day before today, i.e Trade Date.
--The second report date is always one day before today, i.e. Trade Date
DECLARE @Today DATE = CAST(GETDATE() AS DATE);
DECLARE @ReportDate1 DATE = CASE WHEN @ReportDay = 1 THEN DATEADD(DD,-3,@Today) ELSE DATEADD(DD,-1,@Today) END;
DECLARE @ReportDate2 DATE = DATEADD(DD,-1,@Today);

/****************************************************/
/* MiFID UK */
/****************************************************/

/* MIFID UK Transactions */
WITH
MIFID_UK_Trades AS (
SELECT	 ReportDate
		,count (*) as MiFID_TradesUKCount
FROM	RegReportDB_Prod.dbo.MIFID2_Report
WHERE	ReportDate between @ReportDate1 and @ReportDate2 
		and RegulationReportID = 2
GROUP BY	RegulationReportID, 
			ReportDate),

/* MIFID UK Positions */
MIFID_UK_Hedge AS (
SELECT	 ReportDate
		,count (*) as MiFID_HedgeUKCount
FROM	RegReportDB_Prod.dbo.MIFID2_Hedge_Report
WHERE	ReportDate between @ReportDate1 and @ReportDate2 
		and RegulationReportID = 2
GROUP BY	RegulationReportID, 
			ReportDate),

/****************************************************/
/* MiFID EU */
/****************************************************/

/* MIFID EU Transactions */
MIFID_EU_Trades AS (
SELECT	 ReportDate
		,count (*) as MiFID_TradesEUCount
FROM	RegReportDB_Prod.dbo.MIFID2_Report
WHERE	ReportDate between @ReportDate1 and @ReportDate2
		and RegulationReportID = 1
GROUP BY	RegulationReportID,
			ReportDate),

/* MIFID EU Positions */
MIFID_EU_Hedge AS (
SELECT	 ReportDate
		,count (*) as MiFID_HedgeEUCount
FROM	RegReportDB_Prod.dbo.MIFID2_Hedge_Report
WHERE	ReportDate between @ReportDate1 and @ReportDate2
		and RegulationReportID = 1
GROUP BY	RegulationReportID,
			ReportDate),

/* AUS flow in MiFIR */
MIFIDEU_AUS AS (
SELECT	ReportDate
		,count(*) as MIFIDEU_AUS
FROM	[RegReportDB_Prod].[dbo].[MIFID2_ETORO_Report]
WHERE	ReportDate between @ReportDate1 and @ReportDate2
GROUP BY	 ReportDate),

/****************************************************/
/* EMIR UK */
/****************************************************/

/* EMIR UK Corporate Transactions */
EMIR_UK_Trades AS (
SELECT	ReportDate 
		,count(*) as REFIT_UK_Client_Transactions
FROM	 [RegReportDB_Prod].[dbo].[EMIR3_UK_Refit_Report]
WHERE	ReportDate between @ReportDate1 and @ReportDate2
		and [Counterparty_1] <> '213800GIFQMSV7HROS23'
		and Level = 'TCTN'
		and [OpenORClose] = 'O'
GROUP BY	ReportDate),

/* EMIR UK Corporate Positions */
EMIR_UK_Positions AS (
SELECT	ReportDate
		,count(*) as REFIT_UK_Client_Positions
FROM	[RegReportDB_Prod].[dbo].[EMIR3_UK_Refit_Report]
WHERE	ReportDate between @ReportDate1 and  @ReportDate2
		and [Counterparty_1] <> '213800GIFQMSV7HROS23'
		and Level = 'PSTN'
GROUP BY	ReportDate),

/* EMIR UK Collateral Client side */ 
EMIR_UK_Client_Collateral AS (
SELECT	ReportDate
		,count (*) as Refit_UK_Collateral
FROM	[RegReportDB_Prod].[dbo].[EMIR3_UK_Refit_Report_Collateral_History]
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
GROUP BY	ReportDate),


/****************************************************/
/* REFIT EU */
/****************************************************/

/* REFIT clients Transactions */
REFIT_Client_Trades AS (
SELECT	ReportDate
		,count(*) as REFIT_Transactions_ClientFull
FROM	[RegReportDB_Prod].[dbo].[EMIR2_Refit_Report] WITH (NOLOCK)
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
		and Level LIKE 'TCTN'
		and [OpenORClose] = 'O'
		and RegulationID <> 9
GROUP BY	ReportDate ),

/* REFIT clients Positions */
REFIT_Client_Positions AS (
SELECT	ReportDate
		,count(*) as REFIT_Positions_Client
FROM	[RegReportDB_Prod].[dbo].[EMIR2_Refit_Report] 
WHERE	ReportDate between  @ReportDate1 AND  @ReportDate2
		and RegulationID <> 9
		and Level like 'PSTN'
GROUP BY	ReportDate),

/* REFIT Collateral Client side */ 
REFIT_Client_Collateral AS (
SELECT	ReportDate
		,count (*) as Refit_Collateral_agg
FROM	[RegReportDB_Prod].[dbo].[EMIR2_Report_Refit_Collateral]
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
GROUP BY	ReportDate),

/* REFIT ASIC Intercompany Hedge Transactions */
REFIT_ASIC_Trades AS (
SELECT	ReportDate
		,count(*) as REFIT_Transactions_ASIC
FROM	[RegReportDB_Prod].[dbo].[EMIR2_ETORO_Refit_Trades] WITH (NOLOCK)
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
		and Level like 'TCTN'
		and [OpenORClose] = 'O'
GROUP BY	ReportDate),

/* REFIT ASIC Intercompany Hedge Positions */
REFIT_ASIC_Positions AS (
SELECT	ReportDate
		,count(*) as REFIT_Positions_ASIC
FROM	[RegReportDB_Prod].[dbo].[EMIR2_ETORO_Refit_Positions] WITH (NOLOCK)
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
		and Level like 'PSTN'
GROUP BY	ReportDate),

/* REFIT Seychelles Intercompany Hedge Transactions */
REFIT_SC_Trades AS (
SELECT ReportDate
		,count(*) as REFIT_Transactions_Seychelles
FROM	[RegReportDB_Prod].[dbo].[EMIR2_Refit_Report] WITH (NOLOCK)
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
		and Level like 'TCTN'
		and [OpenORClose] = 'O'
		and RegulationID = 9
GROUP BY	ReportDate), 

/* REFIT Seychelles Intercompany Hedge Positions */
REFIT_SC_Positions AS (
SELECT	ReportDate
		,count(*) as REFIT_Positions_Seychelles
FROM	[RegReportDB_Prod].[dbo].[EMIR2_Refit_Report] WITH (NOLOCK)
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
		and RegulationID = 9
		and Level like 'PSTN'
GROUP BY	ReportDate),

/****************************************************/
/* ASIC */
/****************************************************/

/* ASIC Transactions */
ASIC_Trades AS (
SELECT	ReportDate
		,count(*) as ASIC_TransCount
FROM	RegReportDB_Prod.dbo.ASIC2_Transactions
WHERE	OpenORClose = 'O' and ReportDate between @ReportDate1 and @ReportDate2
GROUP BY	ReportDate),

/* ASIC Positions */
ASIC_Positions AS (
SELECT	ReportDate
		,count(*) as ASIC_Positions_AGG
FROM	RegReportDB_Prod.dbo.ASIC2_Positions_AGG
WHERE	ReportDate between @ReportDate1 and @ReportDate2
GROUP BY	ReportDate),

/* ASIC Transactions Hedge */
ASIC_Transactions_Hedge AS (
SELECT	ReportDate
		,count(*) as ASIC_Transactions_Hedge
FROM	[RegReportDB_Prod].[dbo].[ASIC2_Transactions_Hedge]
WHERE	ReportDate between @ReportDate1 and @ReportDate2
GROUP BY	ReportDate),

/* ASIC Positions Hedge */
ASIC_Positions_Hedge AS (
SELECT	ReportDate
		,count(*) as ASIC_Positions_Hedge
FROM	[RegReportDB_Prod].[dbo].[ASIC2_Positions_AGG_Hedge]
WHERE	ReportDate between @ReportDate1 and @ReportDate2
GROUP BY	ReportDate),

/* ASIC Collateral */ 
ASIC_Collateral AS (
SELECT	ReportDate
		,count (*) as ASIC_Collateral
FROM	[RegReportDB_Prod].[dbo].[ASIC2_Collateral]
WHERE	ReportDate between  @ReportDate1 and  @ReportDate2
GROUP BY	ReportDate)

SELECT  ASIC_Trades.ReportDate, 
		MIFID_UK_Trades.MiFID_TradesUKCount,
		MIFID_UK_Hedge.MiFID_HedgeUKCount,
		MIFID_EU_Trades.MiFID_TradesEUCount,
		MIFID_EU_Hedge.MiFID_HedgeEUCount,
		MIFIDEU_AUS.MIFIDEU_AUS,
		EMIR_UK_Trades.REFIT_UK_Client_Transactions,
		EMIR_UK_Positions.REFIT_UK_Client_Positions,
		EMIR_UK_Client_Collateral.Refit_UK_Collateral,
		REFIT_Client_Trades.REFIT_Transactions_ClientFull,
		REFIT_Client_Positions.REFIT_Positions_Client,
		REFIT_Client_Collateral.Refit_Collateral_agg,
		REFIT_ASIC_Trades.REFIT_Transactions_ASIC,
		REFIT_ASIC_Positions.REFIT_Positions_ASIC,
		REFIT_SC_Trades.REFIT_Transactions_Seychelles,
		REFIT_SC_Positions.REFIT_Positions_Seychelles,
		ASIC_Trades.ASIC_TransCount,
		ASIC_Positions.ASIC_Positions_AGG,
		ASIC_Transactions_Hedge.ASIC_Transactions_Hedge,
		ASIC_Positions_Hedge.ASIC_Positions_Hedge,
		ASIC_Collateral.ASIC_Collateral
FROM	ASIC_Trades
LEFT JOIN MIFID_UK_Hedge ON MIFID_UK_Hedge.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN MIFID_EU_Trades ON MIFID_EU_Trades.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN MIFID_EU_Hedge ON MIFID_EU_Hedge.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN MIFIDEU_AUS ON MIFIDEU_AUS.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN EMIR_UK_Trades ON EMIR_UK_Trades.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN EMIR_UK_Positions ON EMIR_UK_Positions.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN EMIR_UK_Client_Collateral ON EMIR_UK_Client_Collateral.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_Client_Trades ON REFIT_Client_Trades.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_Client_Positions ON REFIT_Client_Positions.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_Client_Collateral ON REFIT_Client_Collateral.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_ASIC_Trades ON REFIT_ASIC_Trades.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_ASIC_Positions ON REFIT_ASIC_Positions.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_SC_Trades ON REFIT_SC_Trades.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN REFIT_SC_Positions ON REFIT_SC_Positions.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN MIFID_UK_Trades ON MIFID_UK_Trades.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN ASIC_Positions ON ASIC_Positions.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN ASIC_Transactions_Hedge ON ASIC_Transactions_Hedge.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN ASIC_Positions_Hedge ON ASIC_Positions_Hedge.ReportDate=ASIC_Trades.ReportDate
LEFT JOIN ASIC_Collateral ON ASIC_Collateral.ReportDate=ASIC_Trades.ReportDate
order by ReportDate
OPTION (recompile);



--Count for New MiFID EU flow (removing trades for instruments that are not reportable to EU)
WITH MIFID_Total as(
SELECT	 ReportDate
		,count (*) as MiFID_TradesCount
FROM	RegReportDB_Prod.dbo.MIFID2_Report
WHERE	ReportDate BETWEEN @ReportDate1 and @ReportDate2
		and RegulationReportID = 1
GROUP BY	RegulationReportID, 
			ReportDate),

MIFID_EU AS (
SELECT	main.ReportDate
		,count (*) as MiFID_TradesEUCount
FROM	(SELECT a.ReportDate
				,a.RegulationReportID
				,a.InstrumentID
				,a.TransactionReferenceNumber
				,b.IsMifid
		FROM RegReportDB_Prod.dbo.MIFID2_Report a
		LEFT JOIN	RegReportDB_Prod.dbo.Reg_Instruments_SCD b
		ON		a.InstrumentID = b.InstrumentID
		WHERE	1=1
				and a.ReportDate BETWEEN @ReportDate1 and @ReportDate2
				and a.RegulationReportID = 1
				and b.IsMifid=1
		GROUP BY a.ReportDate
				,a.RegulationReportID
				,a.InstrumentID
				,a.TransactionReferenceNumber
				,b.IsMifid) main
GROUP BY main.ReportDate),

MIFID_UK AS (
SELECT	 ReportDate
		,count (*) as MiFID_TradesUKCount
FROM	RegReportDB_Prod.dbo.MIFID2_Report
WHERE	ReportDate BETWEEN @ReportDate1 and @ReportDate2
		and RegulationReportID = 2
GROUP BY	RegulationReportID, 
			ReportDate),


--SELECT    main.ReportDate
--        ,count (*) as MiFID_TradeUK_UKReportable
--FROM    (SELECT a.ReportDate
--                ,a.RegulationReportID
--                ,a.InstrumentID
--                ,a.TransactionReferenceNumber
--                ,b.IsMifid
--        FROM RegReportDB_Prod.dbo.MIFID2_Report a
--        LEFT JOIN    RegReportDB_Prod.dbo.Reg_Instruments_SCD b
--        ON        a.InstrumentID = b.InstrumentID
--        WHERE    1=1
--                and a.ReportDate = '2024-11-18'
--                and a.RegulationReportID = 2
--                and b.IsMifidByFCA=1
--                and ValidTo='9999-12-31') main
--GROUP BY main.ReportDate

MIFID_UK_EUReportable AS(
SELECT    main.ReportDate
        ,count (*) as MiFID_TradeUK_EUReportable
FROM    (SELECT a.ReportDate
                ,a.RegulationReportID
                ,a.InstrumentID
                ,a.TransactionReferenceNumber
                ,b.IsMifid
        FROM RegReportDB_Prod.dbo.MIFID2_Report a
        LEFT JOIN    RegReportDB_Prod.dbo.Reg_Instruments_SCD b
        ON        a.InstrumentID = b.InstrumentID
        WHERE    1=1
                and a.ReportDate BETWEEN @ReportDate1 and @ReportDate2
                and a.RegulationReportID = 2
                and b.IsMifid=1
         GROUP BY a.ReportDate
				,a.RegulationReportID
				,a.InstrumentID
				,a.TransactionReferenceNumber
				,b.IsMifid) main
GROUP BY main.ReportDate)

SELECT MIFID_Total.ReportDate
		,MIFID_Total.MiFID_TradesCount
		,MIFID_EU.MiFID_TradesEUCount
		,MIFID_Total.MiFID_TradesCount - MIFID_EU.MiFID_TradesEUCount AS Difference1
		,MIFID_UK.MiFID_TradesUKCount
		,MIFID_UK_EUReportable.MiFID_TradeUK_EUReportable
		,MIFID_UK.MiFID_TradesUKCount - MIFID_UK_EUReportable.MiFID_TradeUK_EUReportable AS Difference2
FROM MIFID_Total
LEFT JOIN MIFID_EU ON MIFID_Total.ReportDate=MIFID_EU.ReportDate
LEFT JOIN MIFID_UK ON MIFID_Total.ReportDate=MIFID_UK.ReportDate
LEFT JOIN MIFID_UK_EUReportable ON MIFID_Total.ReportDate=MIFID_UK_EUReportable.ReportDate


SELECT [Trade_date],sum(Transaction_Cnt) AS APA_Count
  FROM [RegReportDB_Prod].[dbo].[RegulationAggTrans]
    where [Trade_date] BETWEEN @ReportDate1 and @ReportDate2
  and [Asset class] in ('Stocks', 'ETF')
  and eToroEntity IN ('eToro EU','eToro UK')
  and [CFD/Real] = 'Real'
  and IsMifidByESMA = 1 
  group by [Trade_date]
  order by [Trade_date]
