CREATE TABLE stocks (
    Date Date,
    Stock Int64,
    article_id Int64
)
ENGINE = Log;

CREATE TABLE sales (
    Date Date,
    Country String,
    Qty Int64,
    FinResult Float64,
    article_id Int64
)
ENGINE = Log;

CREATE TABLE report (
    Date Date,
    Country String,
    article_id Int64,
    Qty Int64,
    days Float32,
    avg_sales Float64

)
ENGINE = Log;