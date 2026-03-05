const { Client, GatewayIntentBits } = require('discord.js');
const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent] });

const LOCKDOWN_ROLE_NAME = process.env.LOCKDOWN_ROLE_NAME || 'Member';
const MOD_ROLES = (process.env.MOD_ROLES || 'Admin')
    .split(',')
    .map(role => role.trim())
    .filter(Boolean);

client.on('messageCreate', async message => {
    if (!message.guild) return;
    if (message.author.bot) return;

    // Lockdown command
    if (message.content === '!lockdown') {
        if (!message.member.roles.cache.some(r => MOD_ROLES.includes(r.name))) return;

        const role = message.guild.roles.cache.find(r => r.name === LOCKDOWN_ROLE_NAME);
        if (!role) return message.channel.send('Role not found.');

        for (const channel of message.guild.channels.cache.values()) {
            try {
                await channel.permissionOverwrites.edit(role, {
                    SendMessages: false,
                    AddReactions: false,
                    Connect: false // for voice channels
                });
            } catch (err) {
                console.error(`Failed to update ${channel.id}:`, err);
            }
        }

        message.channel.send('Server is now in lockdown!');
    }

    // Unlock command
    if (message.content === '!unlock') {
        if (!message.member.roles.cache.some(r => MOD_ROLES.includes(r.name))) return;

        const role = message.guild.roles.cache.find(r => r.name === LOCKDOWN_ROLE_NAME);
        if (!role) return message.channel.send('Role not found.');

        for (const channel of message.guild.channels.cache.values()) {
            try {
                await channel.permissionOverwrites.edit(role, {
                    SendMessages: true,
                    AddReactions: true,
                    Connect: true
                });
            } catch (err) {
                console.error(`Failed to update ${channel.id}:`, err);
            }
        }

        message.channel.send('Lockdown lifted!');
    }
});

const token = process.env.DISCORD_BOT_TOKEN;
if (!token) {
    throw new Error('Missing DISCORD_BOT_TOKEN environment variable.');
}

client.login(token);
