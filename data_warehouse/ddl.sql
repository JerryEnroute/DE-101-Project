USE nike_sales;
USE WAREHOUSE compute_wh;

CREATE TABLE dim_category (
    category VARCHAR(255) PRIMARY KEY
);

CREATE TABLE dim_color (
    color_ID VARCHAR(255) PRIMARY KEY,
    TopColor VARCHAR(255),
    color_Description VARCHAR(1000),
    color_Label VARCHAR(255),
    color_Image_url VARCHAR(1000),
    color_FullPrice INT,
    color_CurrentPrice INT,
    color_Discount BOOLEAN,
    color_BestSeller BOOLEAN,
    color_InStock BOOLEAN,
    color_MemberExclusive BOOLEAN,
    color_New BOOLEAN
);

CREATE TABLE dim_products (
    id INT PRIMARY KEY AUTOINCREMENT,
    UID VARCHAR(255),
    cloudProdID VARCHAR(255),
    productID VARCHAR(255),
    shortID VARCHAR(255),
    title VARCHAR(255),
    subtitle VARCHAR(255),
    prod_url VARCHAR(1000),
    prebuildId VARCHAR(10),
    short_description VARCHAR(1000),
    rating NUMBER(1),
    currency VARCHAR(3),
    fullPrice INT,
    currentPrice INT,
    color_id VARCHAR(255) REFERENCES dim_color(color_ID),
    colorNum INT,
    category VARCHAR(255) REFERENCES dim_category(category),
    type VARCHAR(255),
    channel VARCHAR(255),
    GiftCard BOOLEAN,
    Jersey BOOLEAN,
    Launch BOOLEAN,
    MemberExclusive BOOLEAN,
    NBA BOOLEAN,
    NFL BOOLEAN,
    Sustainable BOOLEAN,
    customizable BOOLEAN,
    ExtendedSizing BOOLEAN,
    sale BOOLEAN,
    label VARCHAR(255),
    inStock BOOLEAN,
    ComingSoon BOOLEAN,
    BestSeller BOOLEAN,
    Excluded BOOLEAN
);

CREATE TABLE dim_dates (
    date_id DATE PRIMARY KEY, 
    day NUMBER(2), 
    month NUMBER(2), 
    year NUMBER(4)
);

CREATE TABLE fact_sales (
    id INT PRIMARY KEY AUTOINCREMENT,
    ticket_id INT,
    product_id INT REFERENCES dim_products(id),
    date_id DATE REFERENCES dim_dates(date_id),
    currency VARCHAR(10),
    sales NUMBER(6, 2),
    quantity INT
);
