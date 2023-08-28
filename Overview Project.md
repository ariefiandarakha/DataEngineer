# Data Engineer Project 

# Scenario
Seorang data engineer yang bekerja untuk perusahaan sepeda. Perusahaan menjual dan mendistribusikan produk ke beberapa negara seperti Amerika Serikat, Kanada, Perancis, Jerman, Australia, dan Inggris. Produk yang diperjualkan adalah berbagai macam sepeda seperti sepeda gunung, sepeda jalan ataupun sepeda tour, serta menyediakan _spare part_ juga. Perusahaan ingin membuat suatu database untuk mendata _product_, kategori dan sub-kategori _product_, _customer_, wilayah penjualan, dan juga hasil penjualannya. Output yang diinginkan adalah Dashboard berupa data _customer summary_,_product summary_, dan _top product sales distribution_

# Objective
  1. Membuat _data lake_ yang berisikan beberapa _stable staging_
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
_Control flow_ untuk migrasi data untuk table territorial seperti gambar di bawah:

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
Dengan menggunakan parameter MaxCreateDate untuk mendapatkan yang baru terbentuk. Kemudian menambahkan ETLDate dengan _derived column_ lalu membentuk target table yaitu table DIMProduct dengan query pembentuk sebagai berikut :
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
  
