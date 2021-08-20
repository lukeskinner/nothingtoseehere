const {
    describe, before, beforeEach, it,
} = require('mocha');

process.env.NODE_ENV = 'development';
const { assert, expect } = require('chai');
const logger = require('../../../modules/logger');
const models = require('../../../models/index');
const testUtilities = require('../test-utilities');
const testData = require('./dataset-pruning-service-data.json');

const DatasetPruningService = require('../../../modules/service/dataset-pruning-service');

const datasetPruningService = new DatasetPruningService({ logger });
const importedPruningDelayInMinutes = 5;
const replicatedPruningDelayInMinutes = 5;

const repackedDatasets = {
    expired_dataset_id_1: {
        dataInfo: [{ id: 'expired_data_info_id_1' }],
        offers: [],
        bids: [],
    },
    expired_dataset_id_2: {
        dataInfo: [{ id: 'expired_data_info_id_2' }],
        offers: [{ id: 'expired_offer_id_1' }],
        bids: [],
    },
    expired_dataset_id_3: {
        dataInfo: [{ id: 'expired_data_info_id_3' }],
        offers: [],
        bids: [{ id: 'expired_bid_id_1' }],
    },
    expired_dataset_id_4: {
        dataInfo: [{ id: 'expired_data_info_id_4' }],
        offers: [{ id: 'expired_offer_id_2' }],
        bids: [{ id: 'expired_bid_id_2' }],
    },
    valid_dataset_id_1: {
        dataInfo: [{ id: 'expired_data_info_id_6' }],
        offers: [{ id: 'valid_offer_id_1' }],
        bids: [],
    },
    valid_dataset_id_2: {
        dataInfo: [{ id: 'expired_data_info_id_6' }],
        offers: [{ id: 'valid_offer_id_2' }, { id: 'expired_offer_id_3' }],
        bids: [],
    },
    valid_dataset_id_3: {
        dataInfo: [{ id: 'expired_data_info_id_7' }],
        offers: [],
        bids: [{ id: 'valid_bid_id_1' }],
    },
    valid_dataset_id_4: {
        dataInfo: [{ id: 'expired_data_info_id_8' }],
        offers: [],
        bids: [{ id: 'valid_bid_id_2' }, { id: 'expired_bid_id_4' }],
    },
    valid_dataset_id_5: {
        dataInfo: [{ id: 'expired_data_info_id_9' }],
        offers: [{ id: 'valid_offer_id_3' }],
        bids: [{ id: 'valid_bid_id_3' }],
    },
    valid_dataset_id_6: {
        dataInfo: [{ id: 'expired_data_info_id_10' }],
        offers: [{ id: 'expired_offer_id_4' }],
        bids: [{ id: 'valid_bid_id_4' }],
    },
    valid_dataset_id_7: {
        dataInfo: [{ id: 'expired_data_info_id_11' }],
        offers: [{ id: 'valid_offer_id_4' }],
        bids: [{ id: 'expired_bid_id_5' }],
    },
    valid_dataset_id_8: {
        dataInfo: [{ id: 'valid_data_info_id_1' }],
        offers: [{ id: 'expired_offer_id_5' }],
        bids: [{ id: 'expired_bid_id_6' }],
    },
    valid_dataset_id_9: {
        dataInfo: [{ id: 'valid_data_info_id_2' }, { id: 'expired_data_info_id_12' }],
        offers: [{ id: 'expired_offer_id_6' }],
        bids: [{ id: 'expired_bid_id_7' }],
    },
};

const datasetsIdForPruning = [
    'expired_dataset_id_1',
    'expired_dataset_id_2',
    'expired_dataset_id_3',
    'expired_dataset_id_4',
];
const offerIdForPruning = [
    'expired_offer_id_1',
    'expired_offer_id_2',
    'expired_offer_id_3',
    'expired_offer_id_4',
];
const dataInfoIdForPruning = [
    'expired_data_info_id_1',
    'expired_data_info_id_2',
    'expired_data_info_id_3',
    'expired_data_info_id_4',
];

describe('Dataset pruning service test', () => {
    beforeEach('Setup container', async () => {
        const now = Date.now();
        const expiredTimestamp = now - (2 * importedPruningDelayInMinutes * 60 * 1000);
        const expiredHoldingTimeInMinutes = 1;
        const validHoldingTimeInMinutes = 10;

        Object.keys(repackedDatasets).forEach((key) => {
            const dataset = repackedDatasets[key];
            dataset.dataInfo.forEach((dataInfo) => {
                dataInfo.importTimestamp = dataInfo.id.startsWith('expired') ? expiredTimestamp : now;
            });
            dataset.offers.forEach((offer) => {
                offer.holdingTimeInMinutes = offer.id.startsWith('expired') ? expiredHoldingTimeInMinutes : validHoldingTimeInMinutes;
            });
            dataset.bids.forEach((bid) => {
                bid.holdingTimeInMinutes = bid.id.startsWith('expired') ? expiredHoldingTimeInMinutes : validHoldingTimeInMinutes;
            });
        });
    });

    it('Get ids for pruning method test', async () => {
        const idsForPruning = datasetPruningService.getIdsForPruning(
            repackedDatasets,
            importedPruningDelayInMinutes,
            replicatedPruningDelayInMinutes,
        );

        assert.deepEqual(idsForPruning.dataInfoIdToBeDeleted, dataInfoIdForPruning, 'Wrong datainfo ids for pruning');
        assert.deepEqual(idsForPruning.offerIdToBeDeleted, offerIdForPruning, 'Wrong offer ids for pruning');
        assert.deepEqual(idsForPruning.datasetsToBeDeleted.map(e => e.datasetId), datasetsIdForPruning, 'Wrong dataset ids for pruning');
    });

    it('Low estimated value datasets pruning test, call findLowEstimatedValueDatasets, successful', async () => {
        await testUtilities.recreateDatabase();
        await models.offers.destroy({
            where: {},
            truncate: true,
        });

        const { findLowEstimatedValueDatasetsData } = testData;
        for (const dataInfo of findLowEstimatedValueDatasetsData.data_info) {
            // eslint-disable-next-line no-await-in-loop
            await models.data_info.create({
                data_set_id: dataInfo.data_set_id,
                import_timestamp: new Date(),
                data_provider_wallet: '',
                total_documents: 2,
                root_hash: '',
                data_size: 1,
                origin: '',
            });
        }
        for (const bid of findLowEstimatedValueDatasetsData.bids) {
            // eslint-disable-next-line no-await-in-loop
            await models.bids.create({
                data_set_id: bid.data_set_id,
                offer_id: '',
                dc_node_id: '',
                data_size_in_bytes: '',
                litigation_interval_in_minutes: 1,
                token_amount: 1,
                holding_time_in_minutes: bid.holding_time_in_minutes,
                status: '',
                blockchain_id: '',
                dc_identity: '',
                message: '',
            });
        }
        for (const offer of findLowEstimatedValueDatasetsData.offers) {
            // eslint-disable-next-line no-await-in-loop
            await models.offers.create({
                data_set_id: offer.data_set_id,
                blockchain_id: '',
                message: '',
                status: '',
                global_status: '',
                trac_in_base_currency_used_for_price_calculation: 1,
                gas_price_used_for_price_calculation: '',
                price_factor_used_for_price_calculation: 1,
            });
        }
        for (const purchased of findLowEstimatedValueDatasetsData.purchased_data) {
            // eslint-disable-next-line no-await-in-loop
            await models.purchased_data.create({
                transaction_hash: '',
                data_set_id: purchased.data_set_id,
                offer_id: '',
                blockchain_id: '',
            });
        }

        const result = await datasetPruningService.findLowEstimatedValueDatasets();
        expect(result.length).to.be.equal(5);
    });
});
