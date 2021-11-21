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
            Utilities.denormalizeHex('0xb7dfc7b550239e91b85ecd50eba1b4425d57039f').toLowerCase(),
            Utilities.denormalizeHex('0x7b359c038b2113229fed1a3e91a136303223bcbf').toLowerCase(),
            Utilities.denormalizeHex('0xb1ebb648bf739740bf68ec4c009cda573c2f2c7f').toLowerCase(),
            Utilities.denormalizeHex('0x56ad6ae0c00808d252f919df801351bc08f0c0c6').toLowerCase(),
            Utilities.denormalizeHex('0xe6927de571271f8617c5a7aa5ceae7e16eefc2ce').toLowerCase(),
            Utilities.denormalizeHex('0x62977fbf03e43929c4ae2d4a230e1402e3c3fb92').toLowerCase(),
            Utilities.denormalizeHex('0x91a8dc9bc2cb151d4a53fac8cce53de544683ab4').toLowerCase()
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
