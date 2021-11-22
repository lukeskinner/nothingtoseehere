const Command = require('../command');
const models = require('../../../models/index');
const Utilities = require('../../Utilities');
const constants = require('../../constants');

const { Op } = models.Sequelize;

/**
 * Creates offer on blockchain
 */
class DCOfferChooseCommand extends Command {
    constructor(ctx) {
        super(ctx);
        this.config = ctx.config;
        this.logger = ctx.logger;
        this.blockchain = ctx.blockchain;
        this.minerService = ctx.minerService;
        this.remoteControl = ctx.remoteControl;
        this.replicationService = ctx.replicationService;
        this.remoteControl = ctx.remoteControl;
        this.errorNotificationService = ctx.errorNotificationService;
    }

    /**
     * Executes command and produces one or more events
     * @param command
     */
    async execute(command) {
        const {
            internalOfferId,
            excludedDHs,
            isReplacement,
            dhIdentity,
            handler_id,
            urgent,
            blockchain_id,
        } = command.data;

        const offer = await models.offers.findOne({ where: { id: internalOfferId } });
        offer.status = 'CHOOSING';
        offer.message = 'Choosing wallets for offer';
        await offer.save({ fields: ['status', 'message'] });
        this.remoteControl.offerUpdate({
            id: internalOfferId,
        });

        const replications = await models.replicated_data.findAll({
            where: {
                offer_id: offer.offer_id,
                status: {
                    [Op.in]: ['STARTED', 'VERIFIED'],
                },
            },
        });

        const verifiedReplications = replications.filter(r => r.status === 'VERIFIED');
        if (excludedDHs == null) {
            const action = isReplacement === true ? 'Replacement' : 'Replication';
            this.logger.notify(`${action} window for ${offer.offer_id} is closed. Replicated to ${replications.length} peers. Verified ${verifiedReplications.length}.`);
        }

        let identities = verifiedReplications
            .map(r => Utilities.denormalizeHex(r.dh_identity).toLowerCase());

        if (excludedDHs) {
            const normalizedExcludedDHs = excludedDHs
                .map(excludedDH => Utilities.denormalizeHex(excludedDH).toLowerCase());
            identities = identities.filter(identity => !normalizedExcludedDHs.includes(identity));
        }


        this.logger.notify(identities.length + ' identities before luke messed with it');
        
        let filteredIdentities = [
            Utilities.denormalizeHex('0xB9712dbeD9769ED25500Eb2e123472a86f45e6F7').toLowerCase(),
            Utilities.denormalizeHex('0x6fa67d02fFdFe5c76E701dca07234A0C1c72f06B').toLowerCase(),
            Utilities.denormalizeHex('0x85101Dc7B44268587ADf01431d9f69513CCe35Ba').toLowerCase(),
            Utilities.denormalizeHex('0x9bc66a5e01fbfcb3e804cc60ad80ddc84ee17024').toLowerCase(),
            Utilities.denormalizeHex('0x6b57c811ad9961fb8c576b9be26f1e790e6085f0').toLowerCase(),
            Utilities.denormalizeHex('0xe6144Aa347baC9597C8e26451EDEf6EA086B664C').toLowerCase(),
            Utilities.denormalizeHex('0xd4Ba0E7BaBAbBdbA3ccC69c94186A2e33E3f455D').toLowerCase(),
            Utilities.denormalizeHex('0xE4dC8Ae17F204a8c33C9ab51cA6F6d9a7cec1EEE').toLowerCase(),
            Utilities.denormalizeHex('0x35C0b09278973cf757A0467e16F2A0cFa17a3403').toLowerCase(),
            Utilities.denormalizeHex('0x1350eAdED80b62C616Cf96F168612260eC8Dd8b2').toLowerCase(),
            Utilities.denormalizeHex('0x02871f3d591D46738A788c9A9bB33a7321Df8deD').toLowerCase(),
            Utilities.denormalizeHex('0x3544Ab2BC93601449dD69b25EEE4eaa56fc2d389').toLowerCase(),
            Utilities.denormalizeHex('0x6F143C3216DFf348c893Ec422C19B2212074913f').toLowerCase(),
            Utilities.denormalizeHex('0x287208c9cD13AAd12c85Ed12f8064C853a9844A3').toLowerCase(),
            Utilities.denormalizeHex('0xb25EF9A62377FE5BbAa549432A6560f5Fa8F7645').toLowerCase(),
        ]; 


        identities = identities.filter(identity => filteredIdentities.indexOf(identity) >= 0);

        this.logger.notify(identities.length + ' identities after luke messed with it');


        if (identities.length < 3) {
            throw new Error('Failed to choose holders. Not enough DHs submitted.');
        }

        let task = null;
        let difficulty = null;
        if (isReplacement) {
            task = await this.blockchain
                .getLitigationReplacementTask(offer.offer_id, dhIdentity, blockchain_id).response;
            difficulty = await this.blockchain
                .getLitigationDifficulty(offer.offer_id, dhIdentity, blockchain_id).response;
        } else {
            // eslint-disable-next-line
            task = offer.task;
            difficulty =
                await this.blockchain.getOfferDifficulty(offer.offer_id, blockchain_id).response;
        }
        const handler = await models.handler_ids.findOne({
            where: { handler_id },
        });
        const handler_data = JSON.parse(handler.data);
        handler_data.status = 'MINING_SOLUTION';
        await models.handler_ids.update(
            {
                data: JSON.stringify(handler_data),
            },
            {
                where: { handler_id },
            },
        );

        await this.minerService.sendToMiner(
            task,
            difficulty,
            identities,
            offer.offer_id,
        );
        return {
            commands: [
                {
                    name: 'dcOfferMiningStatusCommand',
                    delay: 0,
                    period: 5000,
                    data: {
                        offerId: offer.offer_id,
                        excludedDHs,
                        isReplacement,
                        dhIdentity,
                        handler_id,
                        blockchain_id,
                    },
                },
            ],
        };
    }

    /**
     * Recover system from failure
     * @param command
     * @param err
     */
    async recover(command, err) {
        const { internalOfferId, handler_id } = command.data;
        const offer = await models.offers.findOne({ where: { id: internalOfferId } });
        offer.status = 'FAILED';
        offer.global_status = 'FAILED';
        offer.message = `Failed to choose holders. Error message: ${err.message}`;
        await offer.save({ fields: ['status', 'message', 'global_status'] });
        this.remoteControl.offerUpdate({
            id: internalOfferId,
        });
        models.handler_ids.update({
            status: 'FAILED',
        }, { where: { handler_id } });

        this.errorNotificationService.notifyError(
            err,
            {
                offerId: offer.offer_id,
                internalOfferId,
                tokenAmountPerHolder: offer.token_amount_per_holder,
                litigationIntervalInMinutes: offer.litigation_interval_in_minutes,
                datasetId: offer.data_set_id,
                holdingTimeInMinutes: offer.holding_time_in_minutes,
            },
            constants.PROCESS_NAME.offerHandling,
        );

        await this.replicationService.cleanup(offer.id);
        return Command.empty();
    }

    /**
     * Builds default command
     * @param map
     * @returns {{add, data: *, delay: *, deadline: *}}
     */
    default(map) {
        const command = {
            name: 'dcOfferChooseCommand',
            delay: this.config.dc_choose_time,
            transactional: false,
        };
        Object.assign(command, map);
        return command;
    }
}

module.exports = DCOfferChooseCommand;
