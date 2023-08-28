/*Create Database BISource dan BIDWH*/

Create Database BISource

Create Database BIDWH

/*Execute Query Dimdate*/

Use BIDWH 

Select * From dimDate

/*Import data source from excel*/

Use BISource

Select * From [dbo].[Currency]
Select * From [dbo].[Customer]
Select * From [dbo].[Product]
Select * From [dbo].[Promotion]
Select * From [dbo].[Territorial]

/*Source to Staging using SSIS*/

Use BIDWH
-- 1. Currency

Select * From BISource.dbo.Currency
Select * From stgCurrency

--2. Customer

Select * From BISource.dbo.Customer
Select * From stgCustomer

Select Max(Convert(varchar, ModifiedDate,121)) as MaxDate
From stgCustomer

With DelRud as
(
Select ROW_NUMBER() OVER(Partition By CustomerKey Order By ETLDate desc) as RowNum,* From stgCustomer
)
Delete From DelRud
Where RowNum !=1

--3. Product

Select * From BISource.dbo.Product
Select * From stgProduct

Select Max(Convert(varchar, ModifieddDate,121)) as MaxDate
From stgProduct

With DelRud as
(
Select ROW_NUMBER() OVER(Partition By ProductKey Order By ETLDate desc) as RowNum,* From stgProduct
)
Delete From DelRud
Where RowNum !=1

--4. Promotion

Select * From BISource.dbo.Promotion
Select * From stgPromotion

--5. Territorial

Select * From BISource.dbo.Territorial
Select *, CONCAT(SalesTerritoryKey,' ',SalesTerritoryAlternateKey,' ',SalesTerritoryRegion,' ', SalesTerritoryCountry,' ',SalesTerritoryGroup) as IDConcat 
From BISource.dbo.Territorial

Select * From stgTerritorial

--6. Sales

Select * From stgSales


/*Staging to Dim using SSIS*/

1. DimCurrency

Select * From stgCurrency
Select * From dimCurrency

IF NOT EXISTS(Select * From SYSOBJECTS where [name]='dimCurrency' and xtype = 'u')
Begin
Create Table [dbo].[dimCurrency](
	CurrencyKey Int Primary Key,
	CurrencyAlternateKey varchar(max),
	CurrencyName varchar(max),
	ETLDate Datetime
)
END

IF NOT EXISTS(Select * From dimCurrency where CurrencyKey IN ('-2'))
Insert into dbo.dimCurrency values
('-2','Unknown','Unknown','1990-01-01 00:00:00:000')

IF NOT EXISTS(Select * From dimCurrency where CurrencyKey IN ('-1'))
Insert into dbo.dimCurrency values
('-1','Unknown','Unknown','1990-01-01 00:00:00:000')

Update dimCurrency
Set CurrencyAlternateKey = ?,
	CurrencyName =?,
	ETLDATE = GETDATE()
Where CurrencyKey = ?


2.  Customer

Select * From stgCustomer
Select * From dimCustomer

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimCustomer' and xtype ='U')
Begin
Create Table dimCustomer (
	CustomerKey INT PRIMARY KEY,
	GeographyKey INT,
	CustomerAlternateKey varchar(50),
	Title varchar(50),
	CustomerName varchar(50),
	NameStyle varchar(50),
	Birthdate Datetime,
	MaritalStatus varchar(50),
	Suffix varchar(50),
	Gender varchar(50),
	EmailAddress varchar(50),
	YearlyIncome Money,
	TotalChildren Int,
	NumberChildrenAtHome INT,
	EnglishEducation varchar(50),
	SpanisEducation varchar(50),
	FrenchEducation varchar(50),
	EnglishOccupation varchar(50),
	SpanishOccupation varchar(50),
	FrenchOccupation varchar(50),
	HouseOwnerFlag int,
	NumberCarsOwned int,
	AddressLine1 varchar(50),
	AdrdressLine2 varchar(50),
	Phone varchar(50),
	DateFirstPurchase Datetime,
	CommuteDistance varchar(50),
	CreateDate Datetime,
	ModifiedDate Datetime,
	ETLDate Datetime
)
END
IF NOT EXISTS(SELECT * FROM dimCustomer where CustomerKey IN ('-2'))
Insert Into dimCustomer Values
(			'-2'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'-2'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
)

IF NOT EXISTS(SELECT * FROM dimCustomer where CustomerKey IN ('-1'))
Insert Into dimCustomer Values
(			'-1'
           ,'-1'
           ,'-1'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'-1'
           ,'-1'
           ,'-1'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'-1'
           ,'-1'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
)

SELECT CONVERT(varchar,MAX(CreateDate),121)AS MaxCreateDate,CONVERT(varchar,MAX(ModifiedDate),121)AS MaxModifiedDate,
CONVERT(varchar,MAX(ETLDate),121)AS MaxETLDate
FROM dimCustomer

Update dimCustomer
Set         [GeographyKey]			=?
           ,[CustomerAlternateKey]	=?
           ,[Title]					=?
           ,[CustomerName]			=?
           ,[NameStyle]				=?
           ,[Birthdate]				=?
           ,[MaritalStatus]			=?
           ,[Suffix]				=?
           ,[Gender]				=?
           ,[EmailAddress]			=?
           ,[YearlyIncome]			=?
           ,[TotalChildren]			=?
           ,[NumberChildrenAtHome]	=?
           ,[EnglishEducation]		=?
           ,[SpanisEducation]		=?
           ,[FrenchEducation]		=?
           ,[EnglishOccupation]		=?
           ,[SpanishOccupation]		=?
           ,[FrenchOccupation]		=?
           ,[HouseOwnerFlag]		=?
           ,[NumberCarsOwned]		=?
           ,[AddressLine1]			=?
           ,[AdrdressLine2]			=?
           ,[Phone]					=?
           ,[DateFirstPurchase]		=?
           ,[CommuteDistance]		=?
           ,[CreateDate]			=?
           ,[ModifiedDate]			=?
           ,[ETLDate]				=?
Where [CustomerKey] =?
           

3. Product

Select *,REPLACE(weight,',','.') as WeightNew From stgProduct
where size is not null or weight is not null
Select size,Weight From dimProduct
where size is not null or weight is not null

Truncate table dimProduct

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimProduct' and xtype ='U')
Begin
Create Table dimProduct (
	[ProductKey] [int] PRIMARY KEY,
	[ProductAlternateKey] [nvarchar](255) NULL,
	[ProductSubcategoryKey] INT NULL,
	[WeightUnitMeasureCode] [nvarchar](255) NULL,
	[SizeUnitMeasureCode] [nvarchar](255) NULL,
	[EnglishProductName] [nvarchar](255) NULL,
	[SpanishProductName] [nvarchar](255) NULL,
	[FrenchProductName] [nvarchar](255) NULL,
	[StandardCost] MONEY NULL,
	[FinishedGoodsFlag] INT NULL,
	[Color] [nvarchar](255) NULL,
	[SafetyStockLevel] INT NULL,
	[ReorderPoint] INT NULL,
	[ListPrice] MONEY NULL,
	[Size] INT NULL,
	[SizeRange] [nvarchar](255) NULL,
	[Weight] Float NULL,
	[DaysToManufacture] INT NULL,
	[ProductLine] [nvarchar](255) NULL,
	[DealerPrice] MONEY NULL,
	[Class] [nvarchar](255) NULL,
	[Style] [nvarchar](255) NULL,
	[ModelName] [nvarchar](255) NULL,
	[EnglishDescription] [nvarchar](255) NULL,
	[FrenchDescription] [nvarchar](255) NULL,
	[ChineseDescription] [nvarchar](255) NULL,
	[ArabicDescription] [nvarchar](255) NULL,
	[HebrewDescription] [nvarchar](255) NULL,
	[ThaiDescription] [nvarchar](255) NULL,
	[GermanDescription] [nvarchar](255) NULL,
	[JapaneseDescription] [nvarchar](255) NULL,
	[TurkishDescription] [nvarchar](255) NULL,
	[StartDate] Datetime NULL,
	[EndDate] Datetime NULL,
	[Status] [nvarchar](255) NULL,
	[CreateDate] Datetime NULL,
	[Modifiedddate] Datetime NULL,
	[ETLDate] Datetime NULL
)

IF NOT EXISTS(SELECT * FROM dimProduct where ProductKey IN ('-2'))
Insert Into dimProduct Values
(           '-2'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'-2'
           ,'-2'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'-2'
           ,'-2'
           ,'Unknown'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
)

IF NOT EXISTS(SELECT * FROM dimProduct where ProductKey IN ('-1'))
Insert Into dimProduct Values
(           '-1'
           ,'-1'
           ,'-1'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'-1'
           ,'-1'
           ,'Not Available'
           ,'-1'
           ,'-1'
           ,'-1'
           ,'-1'
           ,'Not Available'
           ,'-1'
           ,'-1'
           ,'Not Available'
           ,'-1'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'Not Available'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
)

SELECT CONVERT(varchar,MAX(CreateDate),121)AS MaxCreateDate,CONVERT(varchar,MAX(Modifiedddate),121)AS MaxModifiedDate,
CONVERT(varchar,MAX(ETLDate),121)AS MaxETLDate
FROM dimProduct

Update dimProduct
set         [ProductAlternateKey]	=?
           ,[ProductSubcategoryKey]	=?
           ,[WeightUnitMeasureCode]	=?
           ,[SizeUnitMeasureCode]	=?
           ,[EnglishProductName]	=?
           ,[SpanishProductName]	=?
           ,[FrenchProductName]		=?
           ,[StandardCost]			=?
           ,[FinishedGoodsFlag]		=?
           ,[Color]					=?
           ,[SafetyStockLevel]		=?
           ,[ReorderPoint]			=?
           ,[ListPrice]				=?
           ,[Size]					=?
           ,[SizeRange]				=?
           ,[Weight]				=?
           ,[DaysToManufacture]		=?
           ,[ProductLine]			=?
           ,[DealerPrice]			=?
           ,[Class]					=?
           ,[Style]					=?
           ,[ModelName]				=?
           ,[EnglishDescription]	=?
           ,[FrenchDescription]		=?
           ,[ChineseDescription]	=?
           ,[ArabicDescription]		=?
           ,[HebrewDescription]		=?
           ,[ThaiDescription]		=?
           ,[GermanDescription]		=?
           ,[JapaneseDescription]	=?
           ,[TurkishDescription]	=?
           ,[StartDate]				=?
           ,[EndDate]				=?
           ,[Status]				=?
           ,[CreateDate]			=?
           ,[Modifiedddate]			=?
           ,[ETLDate]				=getdate()
where ProductKey=?

4. Product Sub Category

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimProductSubCategory' and xtype ='U')
Begin
Create Table dimProductSubCategory (
	ProductSubCategoryKey INT PRIMARY KEY,
	ProductCategoryKey INT,
	ProductSubCategoryNameEnglish nvarchar(255),
	ProductSubCategoryNameSpanish nvarchar(255),
	ProductSubCategoryNameFrench nvarchar(255),
	ETLDate datetime
)

IF NOT EXISTS(SELECT * FROM dimProductSubCategory where ProductSubCategoryKey IN ('-2'))
Insert Into dimProductSubCategory Values
(			 '-2'
			,'-2'
			,'Unknown'
			,'Unknown'
			,'Unknown'
			,'1900-01-01 00:00:00:000'
)
IF NOT EXISTS(SELECT * FROM dimProductSubCategory where ProductSubCategoryKey IN ('-1'))
Insert Into dimProductSubCategory Values
(           '-1'
			,'-1'
			,'Not Available'
			,'Not Available'
			,'Not Available'
			,'1900-01-01 00:00:00:000'
)

Select Distinct
		ProductSubcategoryKey,
		CASE
			WHEN ProductSubcategoryKey IN (1,2,3) THEN '1'
			WHEN ProductSubcategoryKey IN (4,5,6,7,8,9,10,11,13,15,17,26,27,30,33,34,35,36,37) THEN '2'
			WHEN ProductSubcategoryKey IN (12,14,16) THEN '3'
			WHEN ProductSubcategoryKey IN (18,19,20,21,22,23,24,25,31) THEN '4'
			WHEN ProductSubcategoryKey IN (28,29,32) THEN '5'
		END as ProductCategoryKey,
		CASE
			WHEN ProductSubCategoryKey = 1	THEN 'Mountain Bike'
			WHEN ProductSubCategoryKey = 2	THEN 'Road Bike'
			WHEN ProductSubCategoryKey = 3	THEN 'Touring Bike'
			WHEN ProductSubCategoryKey = 4	THEN 'Handlebars'
			WHEN ProductSubCategoryKey = 5	THEN 'Bottom Bracket'
			WHEN ProductSubCategoryKey = 6	THEN 'Brakes'
			WHEN ProductSubCategoryKey = 7	THEN 'Chain'
			WHEN ProductSubCategoryKey = 8	THEN 'Crankset'
			WHEN ProductSubCategoryKey = 9	THEN 'Derailleur'
			WHEN ProductSubCategoryKey = 10	THEN 'Fork'
			WHEN ProductSubCategoryKey = 11	THEN 'Headset'
			WHEN ProductSubCategoryKey = 12	THEN 'Mountain Frame'
			WHEN ProductSubCategoryKey = 13	THEN 'Pedal'
			WHEN ProductSubCategoryKey = 14	THEN 'Road Frame'
			WHEN ProductSubCategoryKey = 15	THEN 'Seat'
			WHEN ProductSubCategoryKey = 16	THEN 'Touring Frame'
			WHEN ProductSubCategoryKey = 17	THEN 'Wheel'
			WHEN ProductSubCategoryKey = 18	THEN 'Bib-Shorts'
			WHEN ProductSubCategoryKey = 19	THEN 'Cap'
			WHEN ProductSubCategoryKey = 20	THEN 'Half-Finger Gloves'
			WHEN ProductSubCategoryKey = 21	THEN 'Long-Sleeve Logo Jersey'
			WHEN ProductSubCategoryKey = 22	THEN 'Mens Shorts'
			WHEN ProductSubCategoryKey = 23	THEN 'Socks'
			WHEN ProductSubCategoryKey = 24	THEN 'Womens Tights'
			WHEN ProductSubCategoryKey = 25	THEN 'Vest'
			WHEN ProductSubCategoryKey = 26	THEN 'Rack'
			WHEN ProductSubCategoryKey = 27	THEN 'Bike Stand'
			WHEN ProductSubCategoryKey = 28	THEN 'Water Bottle'
			WHEN ProductSubCategoryKey = 29	THEN 'Dissolver'
			WHEN ProductSubCategoryKey = 30	THEN 'Fender Set'
			WHEN ProductSubCategoryKey = 31	THEN 'Helmet'
			WHEN ProductSubCategoryKey = 32	THEN 'Watter Pack'
			WHEN ProductSubCategoryKey = 33	THEN 'Lights'
			WHEN ProductSubCategoryKey = 34	THEN 'Lock'
			WHEN ProductSubCategoryKey = 35	THEN 'Panniers'
			WHEN ProductSubCategoryKey = 36	THEN 'Pump'
			WHEN ProductSubCategoryKey = 37	THEN 'Tire'
		END as ProductSubCategoryNameEnglish,
		CASE
			WHEN ProductSubCategoryKey = 1	THEN 'Bicicleta de montaña'
			WHEN ProductSubCategoryKey = 2	THEN 'Bicicleta de carretera'
			WHEN ProductSubCategoryKey = 3	THEN 'Bicicleta de paseo'
			WHEN ProductSubCategoryKey = 4	THEN 'Manillar'
			WHEN ProductSubCategoryKey = 5	THEN 'Pedalier'
			WHEN ProductSubCategoryKey = 6	THEN 'Frenos'
			WHEN ProductSubCategoryKey = 7	THEN 'Cadena'
			WHEN ProductSubCategoryKey = 8	THEN 'Bielas'
			WHEN ProductSubCategoryKey = 9	THEN 'Desviador'
			WHEN ProductSubCategoryKey = 10	THEN 'Horquilla'
			WHEN ProductSubCategoryKey = 11	THEN 'Direccion'
			WHEN ProductSubCategoryKey = 12	THEN 'Cuadro de montaña'
			WHEN ProductSubCategoryKey = 13	THEN 'Pedal'
			WHEN ProductSubCategoryKey = 14	THEN 'Cuadro de carretera'
			WHEN ProductSubCategoryKey = 15	THEN 'Sill in/asiento'
			WHEN ProductSubCategoryKey = 16	THEN 'Cuadro de paseo'
			WHEN ProductSubCategoryKey = 17	THEN 'Rear Wheel'
			WHEN ProductSubCategoryKey = 18	THEN 'pantalones cortos de hombre'
			WHEN ProductSubCategoryKey = 19	THEN 'Gorra'
			WHEN ProductSubCategoryKey = 20	THEN 'Guantes'
			WHEN ProductSubCategoryKey = 21	THEN 'Jersey con logo en la manga'
			WHEN ProductSubCategoryKey = 22	THEN 'pantalones cortos deportivos para hombre'
			WHEN ProductSubCategoryKey = 23	THEN 'Calceetines'
			WHEN ProductSubCategoryKey = 24	THEN 'Mallas Para mujer'
			WHEN ProductSubCategoryKey = 25	THEN 'Camiseta'
			WHEN ProductSubCategoryKey = 26	THEN 'Rejilla de enganche'
			WHEN ProductSubCategoryKey = 27	THEN 'Soporte Bicicletas'
			WHEN ProductSubCategoryKey = 28	THEN 'Portabotellas'
			WHEN ProductSubCategoryKey = 29	THEN 'Disolvente'
			WHEN ProductSubCategoryKey = 30	THEN 'Conjunto de Guardabarros'
			WHEN ProductSubCategoryKey = 31	THEN 'Casco'
			WHEN ProductSubCategoryKey = 32	THEN 'Paquete de agua'
			WHEN ProductSubCategoryKey = 33	THEN 'Luces'
			WHEN ProductSubCategoryKey = 34	THEN 'Antirrobo'
			WHEN ProductSubCategoryKey = 35	THEN 'Cesta de Paseo'
			WHEN ProductSubCategoryKey = 36	THEN 'Bomba'
			WHEN ProductSubCategoryKey = 37	THEN 'Cubierta'
		END as ProductSubCategoryNameSpanish,
		CASE
			WHEN ProductSubCategoryKey = 1	THEN 'Vélo de montagne'
			WHEN ProductSubCategoryKey = 2	THEN 'Vélo de route'
			WHEN ProductSubCategoryKey = 3	THEN 'Vélo de randonnée'
			WHEN ProductSubCategoryKey = 4	THEN 'Poignées'
			WHEN ProductSubCategoryKey = 5	THEN 'Axe de Pédalier'
			WHEN ProductSubCategoryKey = 6	THEN 'Freins'
			WHEN ProductSubCategoryKey = 7	THEN 'Chaines'
			WHEN ProductSubCategoryKey = 8	THEN 'Pédalier'
			WHEN ProductSubCategoryKey = 9	THEN 'Dérailleur'
			WHEN ProductSubCategoryKey = 10	THEN 'Fourche'
			WHEN ProductSubCategoryKey = 11	THEN 'Jeu de Direction'
			WHEN ProductSubCategoryKey = 12	THEN 'Cadre de Vélo de montagne'
			WHEN ProductSubCategoryKey = 13	THEN 'Pédale'
			WHEN ProductSubCategoryKey = 14	THEN 'Cadre de Vélo de route'
			WHEN ProductSubCategoryKey = 15	THEN 'Selle'
			WHEN ProductSubCategoryKey = 16	THEN 'Cadre de Vélo de randonnée'
			WHEN ProductSubCategoryKey = 17	THEN 'Roue'
			WHEN ProductSubCategoryKey = 18	THEN 'cuissard à bretelles'
			WHEN ProductSubCategoryKey = 19	THEN 'Casquette'
			WHEN ProductSubCategoryKey = 20	THEN 'Gants'
			WHEN ProductSubCategoryKey = 21	THEN 'Mailot Manches avec Logo'
			WHEN ProductSubCategoryKey = 22	THEN 'Cuissards'
			WHEN ProductSubCategoryKey = 23	THEN 'Chaussettes'
			WHEN ProductSubCategoryKey = 24	THEN 'Collants pour Femmes'
			WHEN ProductSubCategoryKey = 25	THEN 'Veste'
			WHEN ProductSubCategoryKey = 26	THEN 'Étagère'
			WHEN ProductSubCategoryKey = 27	THEN 'Support à vélo'
			WHEN ProductSubCategoryKey = 28	THEN 'Bouteille d eau'
			WHEN ProductSubCategoryKey = 29	THEN 'Dissolvant'
			WHEN ProductSubCategoryKey = 30	THEN 'Jeu de Garde-boue'
			WHEN ProductSubCategoryKey = 31	THEN 'Casque'
			WHEN ProductSubCategoryKey = 32	THEN 'Pack d eau'
			WHEN ProductSubCategoryKey = 33	THEN 'Feu'
			WHEN ProductSubCategoryKey = 34	THEN 'Antivol'
			WHEN ProductSubCategoryKey = 35	THEN 'Sachoches'
			WHEN ProductSubCategoryKey = 36	THEN 'Pompe'
			WHEN ProductSubCategoryKey = 37	THEN 'Pneu'
		END as ProductSubCategoryNameFrench
From stgProduct
Where ProductSubcategoryKey is not null

5. Product Category

Select * From dimProductCategory
Truncate table dimProductCategory

Select	distinct
CASE
					WHEN ProductSubcategoryKey IN (1,2,3) THEN '1'
					WHEN ProductSubcategoryKey IN (4,5,6,7,8,9,10,11,13,15,17,26,27,30,33,34,35,36,37) THEN '2'
					WHEN ProductSubcategoryKey IN (12,14,16) THEN '3'
					WHEN ProductSubcategoryKey IN (18,19,20,21,22,23,24,25,31) THEN '4'
					WHEN ProductSubcategoryKey IN (28,29,32) THEN '5'
				END as ProductCategoryKey,
				CASE
					WHEN ProductSubcategoryKey IN (1,2,3) THEN 'Bike Name'
					WHEN ProductSubcategoryKey IN (4,5,6,7,8,9,10,11,13,15,17,26,27,30,33,34,35,36,37) THEN 'Bike Spare Part'
					WHEN ProductSubcategoryKey IN (12,14,16) THEN 'Bike Frame'
					WHEN ProductSubcategoryKey IN (18,19,20,21,22,23,24,25,31) THEN 'Cycling Clothing'
					WHEN ProductSubcategoryKey IN (28,29,32) THEN 'Accessories'
				END as ProductCategoryNameEnglish,
				CASE
					WHEN ProductSubcategoryKey IN (1,2,3) THEN 'Nombre de la bicicleta'
					WHEN ProductSubcategoryKey IN (4,5,6,7,8,9,10,11,13,15,17,26,27,30,33,34,35,36,37) THEN 'Recambio de bicicleta'
					WHEN ProductSubcategoryKey IN (12,14,16) THEN 'El marco de la bicicleta'
					WHEN ProductSubcategoryKey IN (18,19,20,21,22,23,24,25,31) THEN 'Ropa de ciclismo'
					WHEN ProductSubcategoryKey IN (28,29,32) THEN 'Accesorios de ciclismo'
				END as ProductCategoryNameSpanish,
				CASE
					WHEN ProductSubcategoryKey IN (1,2,3) THEN 'Nom du vélo'
					WHEN ProductSubcategoryKey IN (4,5,6,7,8,9,10,11,13,15,17,26,27,30,33,34,35,36,37) THEN 'Vélo Pièce De Rechange'
					WHEN ProductSubcategoryKey IN (12,14,16) THEN 'Cadre de vélo'
					WHEN ProductSubcategoryKey IN (18,19,20,21,22,23,24,25,31) THEN 'Vêtements de cyclisme'
					WHEN ProductSubcategoryKey IN (28,29,32) THEN 'Accessoires de cyclisme'
				END as ProductCategoryNameFrench
From stgProduct
Where ProductSubcategoryKey is not null

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimProductCategory' and xtype ='U')
Begin
Create Table dimProductCategory (
	ProductCategoryKey INT PRIMARY KEY,
	ProductCategoryNameEnglish nvarchar(255),
	ProductCategoryNameSpanish nvarchar(255),
	ProductCategoryNameFrench nvarchar(255)
)
End

IF NOT EXISTS(SELECT * FROM dimProductCategory where ProductCategoryKey IN ('-2'))
Insert Into dimProductCategory Values
(           '-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
)
IF NOT EXISTS(SELECT * FROM dimProductCategory where ProductCategoryKey IN ('-1'))
Insert Into dimProductCategory Values
(           '-1'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
)

Insert into dimProductCategory Values 
('1','Bike Name','Nombre de la bicicleta','Nom du vélo'),
('2','Bike Spare Part','Recambio de bicicleta','Vélo Pièce De Rechange'),
('3','Bike Frame','El marco de la bicicleta','Cadre de vélo'),
('4','Cycling Clothing','Ropa de ciclismo','Vêtements de cyclisme'),
('5','Accessories','Accesorios de ciclismo','Accessoires de cyclisme')


6. Promotion

Select * From stgPromotion
Select * From dimPromotion

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimPromotion' and xtype ='U')
Begin
Create Table dimPromotion (
	[PromotionKey]				[int] PRIMARY KEY,
	[PromotionAlternateKey]		[int] NULL,
	[EnglishPromotionName]		[varchar](50) NULL,
	[SpanishPromotionName]		[varchar](50) NULL,
	[FrenchPromotionName]		[varchar](50) NULL,
	[DiscountPct]				[int] NULL,
	[EnglishPromotionType]		[varchar](50) NULL,
	[SpanishPromotionType]		[varchar](50) NULL,
	[FrenchPromotionType]		[varchar](50) NULL,
	[EnglishPromotionCategory]	[varchar](50) NULL,
	[SpanishPromotionCategory]	[varchar](50) NULL,
	[FrenchPromotionCategory]	[varchar](50) NULL,
	[StartDate]					Datetime NULL,
	[EndDate]					Datetime NULL,
	[MinQty]					[int] NULL,
	[MaxQty]					[int] NULL,
	[CreateDate]				Datetime NULL,
	[ModifiedDate]				Datetime NULL,
	[ETLDate]					[datetime] NULL
)
END

IF NOT EXISTS(SELECT * FROM dimPromotion where PromotionKey IN ('-2'))
Insert Into dimPromotion Values
(           '-2'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'Unknown'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'-2'
           ,'-2'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
)
IF NOT EXISTS(SELECT * FROM dimPromotion where PromotionKey IN ('-1'))
Insert Into dimPromotion Values
(           '-1'
           ,'-1'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'-1'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'Not Available'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'-1'
           ,'-1'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
           ,'1900-01-01 00:00:00:000'
)

SELECT CONVERT(varchar,MAX(CreateDate),121)AS MaxCreateDate,CONVERT(varchar,MAX(ModifiedDate),121)AS MaxModifiedDate,
CONVERT(varchar,MAX(ETLDate),121)AS MaxETLDate
FROM dimPromotion

Update dimPromotion
Set [PromotionAlternateKey]		=?,
	[EnglishPromotionName]		=?,
	[SpanishPromotionName]		=?,
	[FrenchPromotionName]		=?,
	[DiscountPct]				=?,
	[EnglishPromotionType]		=?,
	[SpanishPromotionType]		=?,
	[FrenchPromotionType]		=?,
	[EnglishPromotionCategory]	=?,
	[SpanishPromotionCategory]	=?,
	[FrenchPromotionCategory]	=?,
	[StartDate]					=?,
	[EndDate]					=?,
	[MinQty]					=?,
	[MaxQty]					=?,
	[CreateDate]				=?,
	[ModifiedDate]				=?,
	[ETLDate]					=?
Where PromotionKey=?


7. Territorial

Select * From stgTerritorial
Select * From dimsalesterritory

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimSalesTerritory' and xtype ='U')
Begin
Create Table dimSalesTerritory (
	[SalesTerritoryKey]				[int] PRIMARY KEY,
	[SalesTerritoryAlternateKey]	[int] NULL,
	[SalesTerritoryRegion]			[varchar](50) NULL,
	[SalesTerritoryCountry]			[varchar](50) NULL,
	[SalesTerritoryGroup]			[varchar](50) NULL,
	[ETLDate]						[datetime] NULL

) END

IF NOT EXISTS(SELECT * FROM dimSalesTerritory where [SalesTerritoryKey] IN ('-2'))
Insert Into dimSalesTerritory Values
(           '-2'
           ,'-2'
           ,'Unknown'
           ,'Unknown'
		   ,'Unknown'
		   ,'1900-01-01 00:00:00:000'
)

IF NOT EXISTS(SELECT * FROM dimSalesTerritory where [SalesTerritoryKey] IN ('-1'))
Insert Into dimSalesTerritory Values
(           '-1'
           ,'-1'
           ,'Not Available'
           ,'Not Available'
		   ,'Not Available'
		   ,'1900-01-01 00:00:00:000'
)

SELECT CONVERT(varchar,MAX(CreateDate),121)AS MaxCreateDate,CONVERT(varchar,MAX(ModifiedDate),121)AS MaxModifiedDate,
CONVERT(varchar,MAX(ETLDate),121)AS MaxETLDate
FROM dimSalesTerritory

8. Sales

Select * From stgSales
Select * From FactSales
Select * From dimDate
where DateKey='20101230'

IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'FactSales' and xtype ='U')
Begin
Create Table FactSales (
	[SalesKey] [int] PRIMARY KEY,
	[ProductKey] [int] NULL,
	[OrderDateKey] [int] NULL,
	[DueDateKey] [int] NULL,
	[ShipDateKey] [int] NULL,
	[CustomerKey] [int] NULL,
	[PromotionKey] [int] NULL,
	[CurrencyKey] [int] NULL,
	[SalesTerritoryKey] [int] NULL,
	[SalesOrderNumber] [varchar](50) NULL,
	[SalesOrderLineNumber] [int] NULL,
	[RevisionNumber] [int] NULL,
	[OrderQuantity] [int] NULL,
	[UnitPrice] [money] NULL,
	[ExtendedAmount] [money] NULL,
	[UnitPriceDiscountPct] [int] NULL,
	[DiscountAmount] [int] NULL,
	[Freight] [money] NULL,
	[SalesAmount] Money NULL,
	[TaxAmount] Money NULL,
	[OrderDate] [datetime] NULL,
	[DueDate] [datetime] NULL,
	[ShipDate] [datetime] NULL,
	[ETLDate] [datetime] NULL
)

Select * From FactSales
Where ETLDate >'2022-06-22 10:19:47.527'

Select max(convert(varchar,etldate,121)) as MaxETLDate
From FactSales
Select max(convert(varchar,etldate,121)) as MaxETLDate
From stgSales

Select SUBSTRING(SalesOrderNumber,3,5) as SalesKey,(UnitPrice*OrderQuantity) as SalesAmount, ((UnitPrice*OrderQuantity)*0.08 )as TaxAmount,* From stgSales
Where convert(varchar,etldate,121)>='2022-06-22 10:19:47.527'
