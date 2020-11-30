const BN = require('bn.js');

const Command = require('../command');
const Utilities = require('../../Utilities');
const Models = require('../../../models');

/**
 * Handles data location response.
 */
class DvPurchaseRequestCommand extends Command {
    constructor(ctx) {
        super(ctx);
        this.remoteControl = ctx.remoteControl;
        this.config = ctx.config;
        this.logger = ctx.logger;
        this.transport = ctx.transport;
        this.profileService = ctx.profileService;
        this.blockchain = ctx.blockchain;
    }

    /**
     * Executes command and produces one or more events
     * @param command
     * @param transaction
     */
    async execute(command, transaction) {
        const {
            data_set_id,
            handler_id,
            ot_object_id,
            seller_node_id,
            blockchain_id,
        } = command.data;

        this.remoteControl.purchaseStatus('Initiating purchase', 'Your purchase request is being processed. Please be patient.');

        const seller_filter = {
            data_set_id,
            ot_json_object_id: ot_object_id,
            seller_node_id,
        };
        if (blockchain_id) {
            seller_filter.blockchain_id = blockchain_id;
        }
        const dataSeller = await Models.data_sellers.findAll({ where: seller_filter });

        if (!dataSeller || !Array.isArray(dataSeller) || dataSeller.length === 0) {
            const error = `Unable to find data seller info with params: ${data_set_id}, ${ot_object_id}, ${seller_node_id}`;
            if (blockchain_id) {
                error.concat(`, ${blockchain_id}`);
            }
            this.logger.error(error);
            await Models.handler_ids.update(
                {
                    status: 'FAILED',
                    data: JSON.stringify({
                        error,
                    }),
                },
                {
                    where: {
                        handler_id,
                    },
                },
            );

            this.remoteControl.purchaseStatus('Purchase initiation failed', 'Unable to find data seller. Please try again.', true);
            return Command.empty();
        }

        const dataTrades = await Models.data_trades.findAll({
            where: {
                data_set_id,
                ot_json_object_id: ot_object_id,
                seller_node_id,
            },
        });

        if (dataTrades && dataTrades.length > 0) {
            const dataTrade = dataTrades.find(dataTrade => dataTrade.status !== 'FAILED');
            if (dataTrade) {
                const errorMessage = `Data purchase already completed or in progress! Previous purchase status: ${dataTrade.status}`;
                await this._handleError(errorMessage, handler_id);
                return Command.empty();
            }
        }

        let selected_price = new BN(dataSeller[0].price);
        let selected_blockchain_id = dataSeller[0].blockchain_id;
        if (!blockchain_id) {
            for (const seller of dataSeller) {
                const seller_price = new BN(seller.price);
                if (seller_price.lt(selected_price)) {
                    selected_price = seller_price;
                    selected_blockchain_id = seller.blockchain_id;
                }
            }
        }

        const { node_wallet, node_private_key } =
            this.blockchain.getWallet(selected_blockchain_id).response;

        // todo pass blockchain identity
        const message = {
            data_set_id,
            dv_erc725_identity: this.profileService.getIdentity(selected_blockchain_id),
            handler_id,
            ot_json_object_id: ot_object_id,
            price: selected_price,
            blockchain_id: selected_blockchain_id,
            wallet: node_wallet,
        };
        const dataPurchaseRequestObject = {
            message,
            messageSignature: Utilities.generateRsvSignature(
                message,
                node_private_key,
            ),
        };


        await this.transport.sendDataPurchaseRequest(
            dataPurchaseRequestObject,
            seller_node_id,
        );

        await Models.data_trades.create({
            data_set_id,
            blockchain_id,
            ot_json_object_id: ot_object_id,
            buyer_node_id: this.config.identity,
            buyer_erc_id: this.profileService.getIdentity(selected_blockchain_id),
            seller_node_id,
            seller_erc_id: dataSeller.seller_erc_id,
            price: dataSeller.price,
            status: 'REQUESTED',
        });

        return Command.empty();
    }

    /**
     * Recover system from failure
     * @param command
     * @param err
     */
    async recover(command, err) {
        const { handler_id } = command.data;
        await this._handleError(`${err}.`, handler_id);
        return Command.empty();
    }

    async _handleError(errorMessage, handler_id) {
        this.logger.error(`Purchase initiation failed. ${errorMessage}`);
        await Models.handler_ids.update(
            {
                status: 'FAILED',
                data: JSON.stringify({
                    errorMessage: `Purchase initiation failed. ${errorMessage}`,
                }),
            },
            {
                where: {
                    handler_id,
                },
            },
        );

        this.remoteControl.purchaseStatus('Purchase initiation failed', errorMessage, true);
    }


    /**
     * Builds default DvPurchaseRequestCommand
     * @param map
     * @returns {{add, data: *, delay: *, deadline: *}}
     */
    default(map) {
        const command = {
            name: 'dvPurchaseRequestCommand',
            delay: 0,
            transactional: false,
        };
        Object.assign(command, map);
        return command;
    }
}

module.exports = DvPurchaseRequestCommand;
