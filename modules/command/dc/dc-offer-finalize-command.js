const BN = require('../../../node_modules/bn.js/lib/bn');

const Command = require('../command');
const Utilities = require('../../Utilities');
const constants = require('../../constants');

const Models = require('../../../models/index');

const { Op } = Models.Sequelize;


/**
 * Finalizes offer on blockchain
 */
class DCOfferFinalizeCommand extends Command {
    constructor(ctx) {
        super(ctx);
        this.config = ctx.config;
        this.logger = ctx.logger;
        this.dcService = ctx.dcService;
        this.blockchain = ctx.blockchain;
        this.remoteControl = ctx.remoteControl;
        this.replicationService = ctx.replicationService;
        this.profileService = ctx.profileService;
        this.errorNotificationService = ctx.errorNotificationService;
    }

    /**
     * Executes command and produces one or more events
     * @param command
     */
    async execute(command) {
        const {
            offerId,
            solution,
            handler_id,
            urgent,
            blockchain_id,
        } = command.data;

        const nodeIdentifiers = solution.nodeIdentifiers.map(ni =>
            Utilities.normalizeHex(ni).toLowerCase());
        const replications = await Models.replicated_data.findAll({
            where: {
                offer_id: offerId,
                dh_identity: { [Op.in]: nodeIdentifiers },
            },
        });

        const colors = [];
        const confirmations = [];


        let is0Mine = false;
        let is1Mine = false;
        let is2Mine = false;


        let filteredIdentities = [
            '0xB9712dbeD9769ED25500Eb2e123472a86f45e6F7'.toLowerCase(),
            '0x6fa67d02fFdFe5c76E701dca07234A0C1c72f06B'.toLowerCase(),
            '0x85101Dc7B44268587ADf01431d9f69513CCe35Ba'.toLowerCase(),
            '0x9bc66a5e01fbfcb3e804cc60ad80ddc84ee17024'.toLowerCase(),
            '0x6b57c811ad9961fb8c576b9be26f1e790e6085f0'.toLowerCase(),
            '0xe6144Aa347baC9597C8e26451EDEf6EA086B664C'.toLowerCase(),
            '0xd4Ba0E7BaBAbBdbA3ccC69c94186A2e33E3f455D'.toLowerCase(),
            '0xE4dC8Ae17F204a8c33C9ab51cA6F6d9a7cec1EEE'.toLowerCase(),
            '0x35C0b09278973cf757A0467e16F2A0cFa17a3403'.toLowerCase(),
            '0x1350eAdED80b62C616Cf96F168612260eC8Dd8b2'.toLowerCase(),
            '0x02871f3d591D46738A788c9A9bB33a7321Df8deD'.toLowerCase(),
            '0x3544Ab2BC93601449dD69b25EEE4eaa56fc2d389'.toLowerCase(),
            '0x6F143C3216DFf348c893Ec422C19B2212074913f'.toLowerCase(),
            '0x287208c9cD13AAd12c85Ed12f8064C853a9844A3'.toLowerCase(),
            '0xb25EF9A62377FE5BbAa549432A6560f5Fa8F7645'.toLowerCase(),
            '0xF5F8E9aA431a3fD6c38bc378Db30072296fe68F2'.toLowerCase(),
            '0x945061816d0e6163bA79F52E45E779A7B3808aEC'.toLowerCase(),
            '0x9A36Ede7d5D501F4Ad80A349a53E3BAB05fF3c2B'.toLowerCase(),
            '0x2525ffc8C8F5C4322bb4174744c63D846059968D'.toLowerCase(),
            '0xF3c9EC4eFbfca36c7d3Ec6aE597d36287c88E24b'.toLowerCase(),
            '0xcB3CD80B51C39369C48532d656c4a03ACDd54F16'.toLowerCase(),
            '0x76E255AFD526d5fd4a8b2D4C0B5b89E92bf1A95b'.toLowerCase(),
            '0x72E4324C780052bB3fe85aa1A01c0917cFD805f6'.toLowerCase(),
            '0x37F9e663981349d3bc0B5833b5Dc65d3a9a43311'.toLowerCase(),
            '0x2d5999924eCa5b0c2909aAcACD3041C84C818699'.toLowerCase(),
            '0x96673292B332F21958e2E85599E76972F4996A35'.toLowerCase(),
            '0xC1Ab1960dE02C376058C764ee0ECE0392c5fedeA'.toLowerCase(),
            '0x0B1F24cc1aEE95e27724300B987824A290A7243f'.toLowerCase(),
            '0x6c123C1eF89BCa0725B573948F44B095dFe563F1'.toLowerCase(),
            '0xd3AA0D7e07BfEE76c7D572CDf8609a07a6Cd0C5e'.toLowerCase()
        ];


        

        let i = 0;
        for (const identity of nodeIdentifiers) {

            this.logger.warn('Debug: ' + identity);

            if (filteredIdentities.indexOf(identity) >= 0) {
                if (i == 0) {
                    is0Mine = true;
                }
                else if (i == 1) {
                    is1Mine = true;
                }
                else if (i == 2) {
                    is2Mine = true;
                }
            }

            const replication = replications.find(r => identity.includes(r.dh_identity));
            colors.push(replication.color);
            confirmations.push(replication.confirmation);

            i++;
        }

        if (!is0Mine || !is1Mine || !is2Mine) {
            this.logger.warn('Blocked finalize of job as the winners are not my nodes.');
            return;
        }

        this.logger.warn('Allowing job to be finalized at the winners are my nodes.')


        const parentIdentity = this.config.parentIdentity ?
            Utilities.normalizeHex(this.config.parentIdentity) : new BN(0, 16);

        const handler = await Models.handler_ids.findOne({
            where: { handler_id },
        });
        const handler_data = JSON.parse(handler.data);
        handler_data.status = 'FINALIZING_OFFER';
        await Models.handler_ids.update(
            {
                data: JSON.stringify(handler_data),
            },
            {
                where: { handler_id },
            },
        );
        let result;
        try {
            result = await this.blockchain.finalizeOffer(
                this.profileService.getIdentity(blockchain_id),
                offerId,
                new BN(solution.shift, 10),
                confirmations[0],
                confirmations[1],
                confirmations[2],
                colors,
                nodeIdentifiers,
                parentIdentity,
                urgent,
                blockchain_id,
            ).response;
        } catch (error) {
            if (error.message.includes('Gas price higher than maximum allowed price')) {
                const delay = constants.GAS_PRICE_VALIDITY_TIME_IN_MILLS / 60 / 1000;
                this.logger.warn(`Gas price too high, delaying call for ${delay} minutes`);

                const handler = await Models.handler_ids.findOne({
                    where: { handler_id },
                });
                const handler_data = JSON.parse(handler.data);
                handler_data.status = 'DELAYED';
                handler.timestamp = Date.now();
                handler.data = JSON.stringify(handler_data);
                await handler.save({ fields: ['data', 'timestamp'] });

                const message = `Offer finalization for offer_id ${offerId} on chain ` +
                    `${blockchain_id} has been delayed at ${(new Date(Date.now())).toUTCString()} due to high gas price`;
                await Models.offers.update({ message }, { where: { offer_id: offerId } });

                return Command.repeat();
            }
            throw error;
        }
        const offer = await Models.offers.findOne({ where: { offer_id: offerId } });
        offer.offer_finalize_transaction_hash = result.transactionHash;
        await offer.save({ fields: ['offer_finalize_transaction_hash'] });

        await Models.handler_ids.update({ timestamp: Date.now() }, { where: { handler_id } });

        return {
            commands: [
                {
                    name: 'dcOfferFinalizedCommand',
                    period: 5000,
                    data: {
                        offerId, nodeIdentifiers, handler_id, blockchain_id,
                    },
                },
            ],
        };
    }

    /**
     * Try to recover command
     * @param command
     * @param err
     * @return {Promise<{commands: *[]}>}
     */
    async recover(command, err) {
        const {
            offerId,
            solution,
            handler_id,
        } = command.data;

        const offer = await Models.offers.findOne({ where: { offer_id: offerId } });
        const excludedDHs = await this.dcService.checkDhFunds(
            solution.nodeIdentifiers,
            offer.token_amount_per_holder,
        );
        if (excludedDHs.length > 0) {
            // send back to miner
            this.logger.important(`DHs [${excludedDHs}] don't have enough funds for offer ${offerId}. Sending back to miner...`);
            const { data } = command;
            if (data.excludedDHs) {
                data.excludedDHs.push(...excludedDHs);
            } else {
                Object.assign(data, {
                    excludedDHs,
                    internalOfferId: offer.id,
                });
            }
            this.logger.warn(`Failed to finalize offer ${offerId} because some of the DHs didn't have enough funds. Trying again...`);
            return {
                commands: [{
                    name: 'dcOfferChooseCommand',
                    data,
                    transactional: false,
                }],
            };
        }

        let errorMessage = err.message;

        const hasFunds = await this.dcService
            .hasProfileBalanceForOffer(offer.token_amount_per_holder);
        if (!hasFunds) {
            errorMessage = 'Not enough tokens. To replicate data please deposit more tokens to your profile';
        }
        err.message = errorMessage;
        return this.invalidateOffer(command, err);
    }

    /**
     * Execute strategy when event is too late
     * @param command
     */
    async expired(command) {
        return this.invalidateOffer(
            command,
            Error('The offer finalization command is too late.'),
        );
    }

    async invalidateOffer(command, err) {
        const {
            offerId,
            solution,
            handler_id,
        } = command.data;
        this.logger.error(`Offer ${offerId} has not been finalized. ${err}`);

        const errorData = {
            offerId,
        };

        const offer = await Models.offers.findOne({ where: { offer_id: offerId } });
        if (offer) {
            offer.status = 'FAILED';
            offer.global_status = 'FAILED';
            offer.message = `Offer ${offerId} has not been finalized. ${err.message}`;
            await offer.save({ fields: ['status', 'message', 'global_status'] });

            errorData.tokenAmountPerHolder = offer.token_amount_per_holder;
            errorData.litigationIntervalInMinutes = offer.litigation_interval_in_minutes;
            errorData.datasetId = offer.data_set_id;
            errorData.holdingTimeInMinutes = offer.holding_time_in_minutes;

            await this.replicationService.cleanup(offer.id);
        } else {
            this.logger.warn(`Offer ${offerId} not found in database.`);
        }

        this.remoteControl.offerUpdate({
            offer_id: offerId,
        });

        Models.handler_ids.update({ status: 'FAILED' }, { where: { handler_id } });

        this.errorNotificationService.notifyError(
            err,
            errorData,
            constants.PROCESS_NAME.offerHandling,
        );

        return Command.empty();
    }

    /**
     * Builds default command
     * @param map
     * @returns {{add, data: *, delay: *, deadline: *}}
     */
    default(map) {
        const command = {
            name: 'dcOfferFinalizeCommand',
            delay: 0,
            period: constants.GAS_PRICE_VALIDITY_TIME_IN_MILLS,
            deadline_at: Date.now() + (5 * constants.GAS_PRICE_VALIDITY_TIME_IN_MILLS),
            transactional: false,
        };
        Object.assign(command, map);
        return command;
    }
}

module.exports = DCOfferFinalizeCommand;
