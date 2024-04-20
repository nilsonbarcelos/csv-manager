const fs = require('file-system')
const { parse } = require("csv-parse")
const { stringify } = require("csv-stringify")

const productList = []
const categoryList = []

const processProductFile = async () => {
    return new Promise((resolve, reject) => {
        const path = './amazon_products.csv';
        const stream = fs.createReadStream(path);
        const parser = parse();
        stream.on('ready', () => {
            console.log('Reading product file')
            stream.pipe(parser);
        });
        parser.on('readable', function () {
            let record;
            while (record = parser.read()) {
                let categoryId = parseInt(record[8])
                if(Number.isInteger(categoryId)){
                    obj = {
                        "productId": record[0],
                        "productName": record[1],
                        "productCategoryId": categoryId,
                    }
                    productList.push(obj)
                }
            }
        });
        parser.on('error', function (err) {
            console.error(err.message)
            reject();
        });
        parser.on('end', function () {
            console.log('Product process completed')
            resolve();
        });
    });
}

const processCategoryProductFile = async () => {
    return new Promise((resolve, reject) => {
        const path = './amazon_categories.csv';
        const stream = fs.createReadStream(path);
        const parser = parse();
        stream.on('ready', () => {
            console.log('Reading category file...')
            stream.pipe(parser);
        });
        parser.on('readable', function () {
            let record;
            while (record = parser.read()) {
                let categoryId = parseInt(record[0])
                if(Number.isInteger(categoryId)){
                    obj = {
                        "categorytId": categoryId,
                        "categoryName": record[1]
                    }
                    categoryList.push(obj)
                }
            }
        });
        parser.on('error', function (err) {
            console.error(err.message);
            reject();
        });
        parser.on('end', function () {
            console.log('Category process completed')
            resolve()
        });
    });
}

productCategoryList = []

const downloadFile = async () => {
    let columns = {
        productId: 'productId',
        productName: 'productName',
        categoryName: 'categoryName',
    }
    stringify(productCategoryList, { header: true, columns: columns }, (err, output) => {
        if (err) throw err;
        fs.writeFile(`product-category.csv`, output, (err) => {
            if (err) throw err;
            console.log(`product-category.csv.`);
        })
    })
}

const run = async()=> {
    await processProductFile()
    await processCategoryProductFile()
    productList.forEach(product => {
        categoryList.forEach(category => {
            if (product.productCategoryId == category.categorytId){
                productCategory = [product.productId, product.productName, category.categoryName]
                productCategoryList.push(productCategory)
            }
        })
    })
    await downloadFile()
}

run()
