const ethers = require('ethers')
const rpc = ''
const provider = new ethers.providers.JsonRpcProvider(rpc)
const fs = require('fs')
const csv = require('fast-csv');
const data = []

const { Client } = require('pg');
const format = require('pg-format');

const host = ''

const main_db = new Client({
    host: host,
    user: '',
    database: '',
    password: '',
    port: 0000,
});

main_db.connect();

const low = require('lowdb')
const FileSync = require('lowdb/adapters/FileSync')

const adapter = new FileSync('g_live.json')
const db = low(adapter)

// Set some defaults
db.defaults({ blocks: [] })
  .write()



// To store the final data (on flashbots blocks that get included)
const storeBlock = (final) => {
    main_db.query(format('QUERY', final),[], (err, result)=>{
        if (err) {
            console.log(err)
        } else {
            console.log("INSERTED")
        }
    })

}


// To fetch how much ETH the fee receipient made by mining that block
const getRegularBlockCoinbaseDiff = async(coinbaseAddress, blockNo) => {
    const minerBalanceBefore = await provider.getBalance(coinbaseAddress, blockNo - 1)
    const minerBalanceAfter = await provider.getBalance(coinbaseAddress, blockNo)
    const minerProfit = minerBalanceAfter.sub(minerBalanceBefore)
    const minerProfitETH = (ethers.utils.formatEther(minerProfit))
    const netCoinbaseDiff = (parseFloat(minerProfitETH))

    return netCoinbaseDiff
}


const main = async () => {

    provider.on((blockNo)=> {
        try{
            const res = await provider.send("eth_getBlockByNumber", ['0x' + blockNo.toString(16), false])
            const balanceChange = await getRegularBlockCoinbaseDiff(res.miner, res.number)

            // db.get('blocks')
            // .push({ blockNumber: i, data: [res, balanceChange]})
            // .write()

            const gasLimit = parseInt(res.gasLimit)
            const gasUsed = parseInt(res.gasUsed)
            const baseFeePerGas = ethers.utils.formatEther(res.baseFeePerGas) // in eth
            const blockHash = res.hash
            const miner = res.miner // fee receipent
            const timestamp = parseInt(res.timestamp)
            const extraData = res.extraData
            const final = [[blockNo, blockHash, timestamp, miner, balanceChange, gasUsed, gasLimit, baseFeePerGas, extraData]]
        }catch(error){
            console.log(error)
        }
    })
}

main()
