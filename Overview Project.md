# Data Engineer Project 

# Scenario
Seorang data engineer yang bekerja untuk perusahaan sepeda. Perusahaan menjual dan mendistribusikan produk ke beberapa negara seperti Amerika Serikat, Kanada, Perancis, Jerman, Australia, dan Inggris. Produk yang diperjualkan adalah berbagai macam sepeda seperti sepeda gunung, sepeda jalan ataupun sepeda tour, serta menyediakan _spare part_ juga. Perusahaan ingin membuat suatu database untuk mendata _product_, kategori dan sub-kategori _product_, _customer_, wilayah penjualan, dan juga hasil penjualannya. Output yang diinginkan adalah Dashboard berupa data _customer summary_,_product summary_, dan _top product sales distribution_

# Objective
  1. Membuat _data lake_ yang berisikan beberapa _table staging_
  2. Mendesain data warehouse (dim dan fact table)
  3. Membuat Visualisasi Data

# Data Lake (staging table)
_Data lake_ dibuat dengan menggunakan _SQL Server Integration Services_ untuk migrasi data dari sumber ke _target table_

## 1. Staging Product Table
_Control flow_ untuk migrasi data untuk table _product_ seperti gambar di bawah:

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/87ff452c-5f3f-40bb-938e-925644b51a88)

Tabel _product_ bersumber dari OLE DB source sql server, dengan menambahkan parameter MaxDate dengan query untuk sebagai berikut :

``` 
Select Max(Convert(varchar, ModifieddDate,121)) as MaxDate From stgProduct
```
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/854b3aed-d479-48d9-9d19-3750fc8dc617)

Lalu untuk _data flow_, _source_ data dibentuk dengan query berikut :
```
Select * From Product
Where convert(varchar,modifiedddate,121)>?
```
Dimana untuk simbol '?' merupakan nilai dari hasil parameter MaxDate yang sebelumnya sudah dibentuk. Kemudian menambahkan kolom ETLDate dengan _derived column_ dengan _command_ GETDATE() agar dapat data di hari terbaru. Yang Kemudian membuat target table dengan membuat table staging baru :
```
CREATE TABLE [stgProduct] (
    [ProductKey] int,
    [ProductAlternateKey] nvarchar(255),
    [ProductSubcategoryKey] nvarchar(255),
    [WeightUnitMeasureCode] nvarchar(255),
    [SizeUnitMeasureCode] nvarchar(255),
    [EnglishProductName] nvarchar(255),
    [SpanishProductName] nvarchar(255),
    [FrenchProductName] nvarchar(255),
    [StandardCost] nvarchar(255),
    [FinishedGoodsFlag] float,
    [Color] nvarchar(255),
    [SafetyStockLevel] float,
    [ReorderPoint] float,
    [ListPrice] nvarchar(255),
    [Size] nvarchar(255),
    [SizeRange] nvarchar(255),
    [Weight] nvarchar(255),
    [DaysToManufacture] float,
    [ProductLine] nvarchar(255),
    [DealerPrice] nvarchar(255),
    [Class] nvarchar(255),
    [Style] nvarchar(255),
    [ModelName] nvarchar(255),
    [EnglishDescription] nvarchar(255),
    [FrenchDescription] nvarchar(255),
    [ChineseDescription] nvarchar(255),
    [ArabicDescription] nvarchar(255),
    [HebrewDescription] nvarchar(255),
    [ThaiDescription] nvarchar(255),
    [GermanDescription] nvarchar(255),
    [JapaneseDescription] nvarchar(255),
    [TurkishDescription] nvarchar(255),
    [StartDate] nvarchar(255),
    [EndDate] nvarchar(255),
    [CreateDate] nvarchar(255),
    [Modifiedddate] nvarchar(255),
    [Status] nvarchar(255),
    [ETLDate] datetime
)
```
Terakhir menambahkan _step delete redundant_ untuk menghindari adanya data duplikasi di tabel _staging_. _Delete Redundant_ dibuat dengan menggunakan query :
```
With DelRud as
(
Select ROW_NUMBER() OVER(Partition By  ProductKey Order By ETLDate desc) as RowNum From stgProduct
)
Delete From DelRud Where RowNum !=1
```

## 2. Staging Customer Table
_Control flow_ untuk migrasi data untuk table customer seperti gambar di bawah:

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/3a9565b9-9f0e-4ede-af00-1138f611a59a)

Sama dengan tabel _product_, tabel _customer_ menggunakan parameter MaxDate agar data yang ditarik hanya data terbaru dan juga menambahkan _delete redundant_.

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/83afe13b-4624-4e42-8154-37753cb8634e)

Kemudian untuk _data flow_, _source_ dibentuk dengan query :
```
Select * From BISource.dbo.Customer
Where Convert(varchar,ModifiedDate,121)>=?
```
Dimana untuk simbol '?' merupakan nilai dari hasil parameter MaxDate yang sebelumnya sudah dibentuk. Kemudian menambahkan kolom ETLDate dengan _derived column_ dengan _command_ GETDATE() agar dapat data di hari terbaru. Yang Kemudian membuat target table dengan membuat table staging baru :
```
CREATE TABLE [stgCustomer] (
    [CustomerKey] int,
    [GeographyKey] float,
    [CustomerAlternateKey] nvarchar(255),
    [Title] nvarchar(255),
    [FirstName] nvarchar(255),
    [MiddleName] nvarchar(255),
    [LastName] nvarchar(255),
    [NameStyle] float,
    [BirthDate] datetime,
    [MaritalStatus] nvarchar(255),
    [Suffix] nvarchar(255),
    [Gender] nvarchar(255),
    [EmailAddress] nvarchar(255),
    [YearlyIncome] float,
    [TotalChildren] float,
    [NumberChildrenAtHome] float,
    [EnglishEducation] nvarchar(255),
    [SpanishEducation] nvarchar(255),
    [FrenchEducation] nvarchar(255),
    [EnglishOccupation] nvarchar(255),
    [SpanishOccupation] nvarchar(255),
    [FrenchOccupation] nvarchar(255),
    [HouseOwnerFlag] float,
    [NumberCarsOwned] float,
    [AddressLine1] nvarchar(255),
    [AddressLine2] nvarchar(255),
    [Phone] nvarchar(255),
    [DateFirstPurchase] datetime,
    [CommuteDistance] nvarchar(255),
    [CreateDate] datetime,
    [ModifiedDate] datetime,
    [ETLDate] datetime
)
```
## 3. Staging Territorial Table
_Control flow_ untuk migrasi data untuk table territorial seperti gambar di bawah:

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/157e920e-f8c3-431b-8a1b-91a25987ee98)

Berbeda dengan tabel _product_ dan tabel _customer_. Tabel Territorial tidak menggunakan parameter MaxDate karena tidak ada kolom time_stamp didalamnya. Oleh karena itu untuk _control flow_ hanya menggunakan _step delete redundant_ untuk menghindari data duplikat.

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/82d9c867-80a6-47e3-b28e-c182f9ff2af9)

Kemudian untuk _data flow_, _source_ dibentuk dengan query :
```
Select *, CONCAT(SalesTerritoryKey,' ',SalesTerritoryAlternateKey,' ',SalesTerritoryRegion,' ', SalesTerritoryCountry,' ',SalesTerritoryGroup) as IDConcat From BISource.dbo.Territorial
```
Dapat dilihat pada query pembentuk data ditambahkan 1 kolom untuk menghasilkan data _unique_ dengan cara menggabungkan seluruh kolom menjadi 1 kolom IDConcat.
Kemudian ditambahkan juga kolom ETLDate seperti tabel-tabel lainnya. Step yang membedakan tabel territorial dengan 2 tabel sebelumnya yaitu menggunakan metode _Lookup Transformator_.
_Lookup_ dibuat dengan mencocokan data yang belum ada dari data sebelumnya (_Redirect rows to no match output_). Dan setelahnya membuat target table dengan membuat table staging baru :
```
CREATE TABLE [stgTerritorial] (
    [SalesTerritoryKey] int,
    [SalesTerritoryAlternateKey] float,
    [SalesTerritoryRegion] nvarchar(255),
    [SalesTerritoryCountry] nvarchar(255),
    [SalesTerritoryGroup] nvarchar(255),
    [IDConcat] nvarchar(815),
    [ETLDate] datetime
)
```

## 4. Staging Sales Table
_Control flow_ untuk migrasi data untuk table sales seperti gambar di bawah:

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/7903aba0-acd2-4fc6-a7cd-7c878b69f557)

Pada tabel ini menggunakan _foreach loop container_ karena source data dari penjualan merupakan beberapa file excel _based on_ bulan dan tahun tertentu.

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/c6c0393c-1986-467a-ab4e-0eb0641528c5)

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/2d2d61f6-046e-4a45-9e18-67c800532f4d)

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/ad986c46-85d3-46ee-8c59-485ba084ca07)

_foreach loop_ ini akan memigrasi dari sumber secara berkali-kali untuk setiap _file_ yang berada di folder sumber tersebut, yang mana setiap data nantinya akan ditampung pada tabel target. Kemudian Sales tetap menggunakan _Delete Redundant_ untuk menghindari adanya duplikasi pada data.

Kemudian untuk _data flow_ ditunjukkan dengan alur seperti gambar di bawah :
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/768dba3c-32f0-4515-ad2f-11daca4062a8)

Dengan alur yang kurang lebih sama yaitu membentuk data dari _source_ yang mana pada tabel Sales ini berupa Excel, kemudian menambahkan ETLDate dengan _derived column_ dan terakhir membuat target table dengan query sebagai berikut :
```
CREATE TABLE [stgSales] (
    [ProductKey] int,
    [OrderDateKey] int,
    [DueDateKey] int,
    [ShipDateKey] int,
    [CustomerKey] int,
    [PromotionKey] int,
    [CurrencyKey] int,
    [SalesTerritoryKey] int,
    [SalesOrderNumber] nvarchar(255),
    [SalesOrderLineNumber] float,
    [RevisionNumber] int,
    [OrderQuantity] int,
    [UnitPrice] money,
    [ExtendedAmount] money,
    [UnitPriceDiscountPct] int,
    [DiscountAmount] int,
    [Freight] money,
    [OrderDate] nvarchar(255),
    [DueDate] nvarchar(255),
    [ShipDate] nvarchar(255),
    [ETLDate] datetime
)
```
Berikut _sample_ data dari ke-empat tabel yang telah dimigrasikan ke tabel _staging_
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/3d5e8d78-f95c-43d4-b4e9-a5d882e24384)

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/c20a0b98-36bd-4217-bbf7-bfe5b7e55bfd)

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/ea84a9af-2f0c-4f3e-b853-3277b36e6a40)

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/f91831d9-5c3a-4bc2-b939-913315da74ff)



# Data Warehouse (dim dan fact table)

## 1. DIM Product
### Control Flow
  
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/c3fe2162-7ea5-4f12-a0c5-39068d364e70)

#### Create Table
Create Table menggunakan query sebagai berikut :
       
```
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
	[Size] [nvarchar](255) NULL,
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
End
 ```
#### Get Variable
Get Variable berfungsi juga untuk membentuk parameter, dalam tabel ini digunakan 3 parameter yaitu MaxCreateDate, MaxModifiedDate, dan MaxETLDate

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/e1dc0c5b-0b90-4464-a37d-70e347b10ddb)

dengan query pembentuk ketiga parameter tersebut yaitu :
```
SELECT CONVERT(varchar,MAX(CreateDate),121)AS MaxCreateDate,CONVERT(varchar,MAX(Modifiedddate),121)AS MaxModifiedDate,
CONVERT(varchar,MAX(ETLDate),121)AS MaxETLDate
FROM dimProduct
```

### Data Flow
#### Insert to DIM Table
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/393c67f1-426a-4ac6-963d-a5c6e0a9f5ac)

Query pembentuk dari _source_ :
```
Select *,REPLACE(weight,',','.') as WeightNew
From stgProduct
Where convert(varchar,createdate,121)>?
```
Dengan menggunakan parameter MaxCreateDate agar mendapatkan data yang baru terbentuk. Kemudian menambahkan ETLDate dengan _derived column_ lalu membentuk target table yaitu table DIMProduct dengan query pembentuk sebagai berikut :
```
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
```

#### Update DIM Table
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/26f52d20-c311-438c-8f4d-b0793e4f7f0e)
  
Query pembentuk dari _source_:
```
Select *,REPLACE(weight,',','.') as WeightNew
From stgProduct
Where convert(varchar,modifiedddate,121)>? and convert(varchar,etldate,121)>?
```
Dengan menggunakan parameter MaxModifiedDate dan MaxETLDate agar mendapatkan data yang terdapat perubahan atau _update_. Kemudian membuat _command_ agar dapat mengupdate data yang berubah dengan query sebagai berikut :
```
Update dimProduct
set         [ProductAlternateKey]	=?
           ,[ProductSubcategoryKey]	=?
           ,[WeightUnitMeasureCode]	=?
           ,[SizeUnitMeasureCode]	=?
           ,[EnglishProductName]	=?
           ,[SpanishProductName]	=?
           ,[FrenchProductName]		=?
           ,[StandardCost]		=?
           ,[FinishedGoodsFlag]		=?
           ,[Color]			=?
           ,[SafetyStockLevel]		=?
           ,[ReorderPoint]		=?
           ,[ListPrice]			=?
           ,[Size]			=?
           ,[SizeRange]			=?
           ,[Weight]			=?
           ,[DaysToManufacture]		=?
           ,[ProductLine]		=?
           ,[DealerPrice]		=?
           ,[Class]			=?
           ,[Style]			=?
           ,[ModelName]			=?
           ,[EnglishDescription]	=?
           ,[FrenchDescription]		=?
           ,[ChineseDescription]	=?
           ,[ArabicDescription]		=?
           ,[HebrewDescription]		=?
           ,[ThaiDescription]		=?
           ,[GermanDescription]		=?
           ,[JapaneseDescription]	=?
           ,[TurkishDescription]	=?
           ,[StartDate]			=?
           ,[EndDate]			=?
           ,[Status]			=?
           ,[CreateDate]		=?
           ,[Modifiedddate]		=?
           ,[ETLDate]			=getdate()
where ProductKey=?
```

## 2. DIM Product Category
### Control Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/f2351a9c-1c74-403b-b85c-4e27df1cc2cd)

#### Create Table dimProductCategory

Query pembentuk table dimProductCategory:
```
IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimProductCategory' and xtype ='U')
Begin
Create Table dimProductCategory (
	ProductCategoryKey INT PRIMARY KEY,
	ProductCategoryNameEnglish nvarchar(255),
	ProductCategoryNameSpanish nvarchar(255),
	ProductCategoryNameFrench nvarchar(255),
	ETLDate datetime
)
End
```

### Data Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/119dd158-fd90-4fd2-b26e-8c1afb58a5b6)

Query pembentuk dari _source_
```
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
```
Kemudian menambahkan kolom ETLDate  dengan _derive column_ lalu dilanjutkan dengan konversi tipe data, dan masuk ke tahap _lookup_ data. Dapat dilihat pada _flow_ terdapat cabang antara _Insert_ dan _Update_ karena kondisi _key_ untuk Insert dan Update berbeda. 
Pada kasus ini, _Primary Key_ yang digunakan adalah _ProductCategoryKey_ yang mana apabila ada kecocokan pada kolom tersebut maka akan dilakukan update, dan apabila tidak ada kecocokan data pada kolom tersebut maka akan dijalankan _command_ insert.

Query update command:
```
Update dimProductCategory
Set
	ProductCategoryNameEnglish=?,
	ProductCategoryNameSpanish=?,
	ProductCategoryNameFrench=?
Where  	ProductCategoryKey=?
```

## 3. DIM Product Sub Category
### Control Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/47dc30c9-13a4-42b1-a779-4362db979f89)

Query pembentuk tabel :
```
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
End
```
### Data Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/1fe4107e-669b-4219-b70c-09c853a1d215)

Query pembentuk dari _source_:
```
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
```
Kemudian menambahkan kolom ETLDate  dengan _derive column_ lalu dilanjutkan dengan konversi tipe data, dan masuk ke tahap _lookup_ data. Dapat dilihat pada _flow_ terdapat cabang antara _Insert_ dan _Update_ karena kondisi _key_ untuk Insert dan Update berbeda. 
Pada kasus ini, _Primary Key_ yang digunakan adalah _ProductSubCategoryKey_ yang mana apabila ada kecocokan pada kolom tersebut maka akan dilakukan update, dan apabila tidak ada kecocokan data pada kolom tersebut maka akan dijalankan _command_ insert.

Query Update Command :
```
Update dimProductSubCategory
Set
	ProductCategoryKey =?,
	ProductSubCategoryNameEnglish =?,
	ProductSubCategoryNameSpanish =?,
	ProductSubCategoryNameFrench =?
Where 	ProductSubCategoryKey =?
```

## 4. DIM Customer
### Control Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/3bac8991-1ced-4f46-9e06-2da6d2d3ade7)

#### Create Table
Create Table menggunakan query sebagai berikut :
```
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
```

#### Get Variable
Get Variable berfungsi juga untuk membentuk parameter, dalam tabel ini digunakan 3 parameter yaitu MaxCreateDate, MaxModifiedDate, dan MaxETLDate

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/294aa1ba-2bd6-4a27-8354-fdfbe0d3b47f)

Query pembentuk ketiga parameter tersebut :
```
SELECT CONVERT(varchar,MAX(CreateDate),121)AS MaxCreateDate,CONVERT(varchar,MAX(ModifiedDate),121)AS MaxModifiedDate,
CONVERT(varchar,MAX(ETLDate),121)AS MaxETLDate
FROM dimCustomer
```

### Data Flow
#### Insert to Dim Customer

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/77be91f3-d416-4789-b685-2e9a53ea9d74)

Query pembentuk dari _source_ :
```
Select *, Concat(firstname,' ',middlename,' ',lastname) as CustomerName From stgCustomer
Where Convert(varchar, createdate, 121)>?
```
Dengan menggunakan parameter MaxCreateDate agar mendapatkan data yang baru terbentuk. Kemudian menambahkan ETLDate dengan _derived column_ lalu membentuk target table yaitu table DIMProduct dengan query pembentuk sebagai berikut :
```
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
```
 
#### Update Dim Customer
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/6c48af98-792a-4784-b880-1fd7923ad34d)

Query pembentuk dari _source_:
```
Select *,concat(firstname,' ',middlename,' ',lastname) as CustomerName From stgCustomer
Where convert(varchar,modifieddate,121)>=? and convert(varchar,etldate,121)>=?
```
Dengan menggunakan parameter MaxModifiedDate dan MaxETLDate agar mendapatkan data yang terdapat perubahan atau _update_. Kemudian membuat _command_ agar dapat mengupdate data yang berubah dengan query sebagai berikut :
```
Update dimCustomer
Set         [GeographyKey]		=?
           ,[CustomerAlternateKey]	=?
           ,[Title]			=?
           ,[CustomerName]		=?
           ,[NameStyle]			=?
           ,[Birthdate]			=?
           ,[MaritalStatus]		=?
           ,[Suffix]			=?
           ,[Gender]			=?
           ,[EmailAddress]		=?
           ,[YearlyIncome]		=?
           ,[TotalChildren]		=?
           ,[NumberChildrenAtHome]	=?
           ,[EnglishEducation]		=?
           ,[SpanisEducation]		=?
           ,[FrenchEducation]		=?
           ,[EnglishOccupation]		=?
           ,[SpanishOccupation]		=?
           ,[FrenchOccupation]		=?
           ,[HouseOwnerFlag]		=?
           ,[NumberCarsOwned]		=?
           ,[AddressLine1]		=?
           ,[AdrdressLine2]		=?
           ,[Phone]			=?
           ,[DateFirstPurchase]		=?
           ,[CommuteDistance]		=?
           ,[CreateDate]		=?
           ,[ModifiedDate]		=?
           ,[ETLDate]			=GETDATE()
Where 	    [CustomerKey] 		=?
```
## 5. DIM Sales Territory
### Control Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/fc443804-315b-4fad-a326-1539f58df701)

Query pembentuk tabel :
```
IF NOT EXISTS(SELECT * FROM SYSOBJECTS where [name] = 'dimSalesTerritory' and xtype ='U')
Begin
Create Table dimSalesTerritory (
	[SalesTerritoryKey] [int] PRIMARY KEY,
	[SalesTerritoryAlternateKey] [int] NULL,
	[SalesTerritoryRegion] [varchar](50) NULL,
	[SalesTerritoryCountry] [varchar](50) NULL,
	[SalesTerritoryGroup] [varchar](50) NULL,
	[ETLDate] [datetime] NULL

) END
```
### Data Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/f4aa5b18-3520-4939-8c07-b77fbbc22b3b)

Query pembentuk dari _source_:
```
Select * From stgTerritorial
```
Kemudian menambahkan kolom ETLDate  dengan _derive column_ lalu dilanjutkan dengan konversi tipe data, dan masuk ke tahap _lookup_ data. Dapat dilihat pada _flow_ terdapat cabang antara _Insert_ dan _Update_ karena kondisi _key_ untuk Insert dan Update berbeda. 
Pada kasus ini, _Primary Key_ yang digunakan adalah _SalesTerritoryKey_ yang mana apabila ada kecocokan pada kolom tersebut maka akan dilakukan update, dan apabila tidak ada kecocokan data pada kolom tersebut maka akan dijalankan _command_ insert.

Query Update Command :
```
Update [dbo].[dimSalesTerritory]
Set [SalesTerritoryAlternateKey] =?,
[SalesTerritoryRegion] =?,		
[SalesTerritoryCountry] =?,		
[SalesTerritoryGroup]=?,
ETLDate=GETDATE()		
Where [SalesTerritoryKey]=?
```

## 6. Fact Sales
### Control Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/1c420408-7187-4796-97fa-990535a9031b)

#### Create Tabel
Query Pembentuk Tabel Fact Sales :
```
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
```
#### Get Variable
Get Variable berfungsi juga untuk membentuk parameter, dalam tabel ini digunakan 1 parameter yaitu MaxETLDate

![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/2bf5f920-7d9e-404a-8b63-a2c504214e77)

Query pembentuk parameter :
```
Select max(convert(varchar,etldate,121)) as MaxETLDate
From FactSales
```

### Data Flow
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/9e50f40a-5569-4042-858d-18e7d2363ee9)

Query pembentuk dari _source_:
```
Select	SUBSTRING(SalesOrderNumber,3,5) as SalesKey,
	IIF(ss.ProductKey is null, -2, IIF(dp.ProductKey is null, -1,dp.ProductKey)) ProductKey,
	IIF(ss.CustomerKey is null, -2, IIF(dc.CustomerKey is null, -1, dc.CustomerKey)) CustomerKey,
	IIF(ss.PromotionKey is null, -2, IIF(dpr.PromotionKey is null, -1, dpr.PromotionKey)) PromotionKey,
	IIF(ss.CurrencyKey is null, -2, IIF(dcr.CurrencyKey is null, -1, dcr.CurrencyKey)) CurrencyKey,
	IIF(ss.SalesTerritoryKey is null, -2, IIF(dst.SalesTerritoryKey is null, -1, dst.SalesTerritoryKey)) SalesTerritoryKey,
	IIF(ss.OrderDateKey is null, -2, IIF(dd.DateKey is null,-1,dd.DateKey)) OrderDateKey,
	ss.DueDate,ss.DueDateKey,ss.ShipDateKey,ss.SalesOrderNumber,ss.SalesOrderLineNumber,ss.RevisionNumber,ss.OrderQuantity,ss.UnitPrice,
	ss.ExtendedAmount,ss.UnitPriceDiscountPct,ss.DiscountAmount,ss.Freight,ss.OrderDate,ss.ShipDate,ss.ETLDate,
	(ss.UnitPrice*ss.OrderQuantity) as SalesAmount, 
	((ss.UnitPrice*ss.OrderQuantity)*0.08 )as TaxAmount
From stgSales as ss
Left Join dimProduct as dp on ss.ProductKey=dp.ProductKey
Left Join dimCustomer as dc on ss.CustomerKey=dc.CustomerKey
Left Join dimPromotion as dpr on ss.PromotionKey=dpr.PromotionKey
Left Join dimCurrency as dcr on ss.CurrencyKey=dcr.CurrencyKey
Left Join dimSalesTerritory as dst on ss.SalesTerritoryKey=dst.SalesTerritoryKey
Left Join dimDate as dd on ss.OrderDateKey=dd.DateKey
Where convert(varchar,ss.etldate,121)>?
```
Kemudian menambahkan kolom ETLDate  dengan _derive column_ lalu dilanjutkan dengan konversi tipe data, dan masuk ke tahap _lookup_ data dengan _SalesKey_  sebagai _unique key_. Apabila _SalesKey_ tersebut sudah ada di tabel maka tidak akan dilakukan apa-apa, namun apabila belum ada di tabel maka akan dilakukan proses _insert_ ke tabel Fact Sales.

# OLAP
Kemudian dilakukan analisis untuk mengukur beberapa KPI yang diperlukan untuk kebutuhan dashboard. Dengan menggunakan data dari FactSales dan penggunaan DAX pada nilai tersebut maka akan didapatkan beberapa KPI seperti yang akan ditunjukkan di bawah.

### Total Sales
```TotalSales:=SUM(FactSales[SalesAmount])```

### Sales Quantity
```SalesQty:=COUNT(FactSales[SalesKey])```

### Sales Last Month
```Sales Last Month:=CALCULATE([TotalSales],DATEADD(dimDate[FullDate],-1,MONTH))```

### Sales Last Year
```Sales Last Year:=CALCULATE([TotalSales], SAMEPERIODLASTYEAR('dimDate'[FullDate]))```

### Total Product Sold
```Total Product Sold:=DISTINCTCOUNT(FactSales[ProductKey])```

### Total Order Quantity
```TotalOrderQuantity:=SUM(FactSales[OrderQuantity])```

### Total Customer
```Total Customer:=COUNT(dimCustomer[CustomerKey])```

### Total Customer Transaction
```Total Customer Transaction:=COUNT(FactSales[CustomerKey])```

### Total Males Customer
```Total Males Customer:=CALCULATE(DISTINCTCOUNT(FactSales[CustomerKey]),dimCustomer[Gender]=="M")```

### Total Female Customer
```Total Females Customer:=CALCULATE(DISTINCTCOUNT(FactSales[CustomerKey]),dimCustomer[Gender]=="F")```

### Total Married Customer
```Total Married Customer:=CALCULATE(DISTINCTCOUNT(FactSales[CustomerKey]),dimCustomer[MaritalStatus]=="M")```

### Total Single Customer
```Total SingleCustomer:=CALCULATE(DISTINCTCOUNT(FactSales[CustomerKey]),dimCustomer[MaritalStatus]=="S")```

# Visualisasi Data

## Customer Sales Summary
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/b12d6727-1b74-4e80-ac63-defbde2cc2a5)

Dapat membantu mengetahui segmentasi dari customer berdasarkan gender, status pernikahan, wilayah. Dan juga mengetahui beberapa KPI seperti :
	1. Total Penjualan
 	2. Total Transaksi
  	3. Total Customer Pria
   	4. Total Customer Wanita
    	5. Total Customer Sudah Menikah
     	6. Total Customer Belum Menikah
      	7. Trend Penjualan per Bulan

## Product Sales Summary
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/b6fd571b-1971-4fb9-ba13-50d0832653b7)

Dapat membantu mengetahui beberapa KPI seperti :
	1. Total banyaknya penjualan produk
 	2. Banyaknya transaksi
  	3. Penjualan produk berdasarkan warna produk
   	4. Trend Penjualan per bulan
    	5. Rincian/Detail Penjualan per produk dalam bulan dan tahun

## Product Trend and Sales Distribution
![image](https://github.com/ariefiandarakha/DataEngineer/assets/70312661/07905389-2f39-4689-b0bd-489f44d60003)

dapat membantu mengetahui informasi mengenai :
	1. Persentase penjualan produk
 	2. Trend penjualan produk setiap bulan dalam setahun
  	3. Penjualan produk berdasarkan dari wilayah
