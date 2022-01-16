import * as csvParser from 'csv-parser'
import * as fs from 'fs'
import { join } from 'path'

const ObjectsToCsv = require('objects-to-csv')

interface RawCatalog {
  SKU: string
  Description: string
}

interface CatalogSupplier {
  SupplierId: string
  SKU: string
  Barcode: string
}

interface MergedCatalog {
  SKU: string
  Description: string
  Source: string
}

const merge = () => {
  const catalogA: RawCatalog[] = []

  let catalogB: RawCatalog[] = []

  const supplierA: CatalogSupplier[] = []

  const supplierB: CatalogSupplier[] = []

  const merged: MergedCatalog[] = []

  fs.createReadStream(join(__dirname, '/../input/catalogA.csv'))
    .pipe(csvParser({}))
    //load catalogA into Array
    .on('data', (row: RawCatalog) => {
      catalogA.push(row)
    })
    .on('end', () => {
      fs.createReadStream(join(__dirname, '/../input/catalogB.csv'))
        .pipe(csvParser({}))
        //load catalogB into Array
        .on('data', (row: RawCatalog) => {
          catalogB.push(row)
        })
        .on('end', () => {
          fs.createReadStream(join(__dirname, '/../input/barcodesA.csv'))
            .pipe(csvParser({}))
            //load BarcodesA into Array
            .on('data', (row: CatalogSupplier) => {
              supplierA.push(row)
            })
            .on('end', () => {
              fs.createReadStream(join(__dirname, '/../input/barcodesB.csv'))
                .pipe(csvParser({}))
                //load BarcodesB into Array
                .on('data', (row: CatalogSupplier) => {
                  supplierB.push(row)
                })
                .on('end', async () => {
                  //map all products from company A
                  catalogA.map(product => {
                    //check supplier A barcode if have matched barcode from supplier B
                    supplierA
                      .filter(p => p.SKU === product.SKU)
                      .map(catalog => {
                        const overlapped = supplierB.find(p => p.Barcode === catalog.Barcode)

                        //if have matched barcode, the product in company B is same with A, remove from B catalog list.
                        if (overlapped) {
                          catalogB = [...catalogB.filter(p => p.SKU !== overlapped.SKU)]
                        }
                      })

                    //add product into merged array
                    merged.push({
                      SKU: product.SKU,
                      Description: product.Description,
                      Source: 'A',
                    })
                  })

                  //now B catalog list have been filtered duplicated products, push rest to the array.
                  catalogB.map(product => {
                    merged.push({
                      SKU: product.SKU,
                      Description: product.Description,
                      Source: 'B',
                    })
                  })

                  //print to file
                  const csv = new ObjectsToCsv(merged)

                  await csv.toDisk(join(__dirname, '/../output/result_output.csv'))

                  console.log('..Done')
                })
            })
        })
    })
}

merge()
