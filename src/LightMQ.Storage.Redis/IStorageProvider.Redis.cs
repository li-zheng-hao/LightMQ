using LightMQ.Options;
using LightMQ.Transport;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace LightMQ.Storage.Redis;

public class RedisStorageProvider : IStorageProvider
{
    private readonly IOptions<LightMQOptions> _mqOptions;
    private readonly IOptions<RedisOptions> _redisOptions;
    private ConnectionMultiplexer? _connection;
    private readonly object _locker = new();

    public RedisStorageProvider(
        IOptions<LightMQOptions> mqOptions,
        IOptions<RedisOptions> redisOptions)
    {
        _mqOptions = mqOptions;
        _redisOptions = redisOptions;
    }

    private ConnectionMultiplexer GetConnection()
    {
        if (_connection is not { IsConnected: true })
        {
            lock (_locker)
            {
                if (_connection is not { IsConnected: true })
                {
                    _connection?.Dispose();
                    _connection = ConnectionMultiplexer.Connect(_redisOptions.Value.ConnectionString);
                }
            }
        }
        return _connection;
    }

    private IDatabase GetDb() => GetConnection().GetDatabase();

    private string MsgKey(string topic, string id) =>
        $"{_redisOptions.Value.KeyPrefix}:{topic}:msg:{id}";

    private string PendingKey(string topic) =>
        $"{_redisOptions.Value.KeyPrefix}:{topic}:pending";

    private string PendingQueueKey(string topic, string queue) =>
        $"{_redisOptions.Value.KeyPrefix}:{topic}:pending:q:{queue}";

    private string ProcessingKey(string topic) =>
        $"{_redisOptions.Value.KeyPrefix}:{topic}:processing";

    private string QueuesKey(string topic) =>
        $"{_redisOptions.Value.KeyPrefix}:{topic}:queues";

    private string MsgKeyPrefix(string topic) =>
        $"{_redisOptions.Value.KeyPrefix}:{topic}:msg:";

    private string ResetLeaseKey =>
        $"{_redisOptions.Value.KeyPrefix}:lightmq:reset:lease";

    private static readonly string NodeId = Guid.NewGuid().ToString("N");

    private TimeSpan MsgExpiry => _mqOptions.Value.MessageExpireDuration;

    #region Publish

    public async Task PublishNewMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var json = JsonConvert.SerializeObject(message);
        var msgKey = MsgKey(message.Topic, message.Id);

        await db.StringSetAsync(msgKey, json, MsgExpiry);

        if (!string.IsNullOrEmpty(message.Queue))
        {
            await db.SortedSetAddAsync(PendingQueueKey(message.Topic, message.Queue), message.Id, message.ExecutableTime.Ticks);
            await db.SetAddAsync(QueuesKey(message.Topic), message.Queue);
        }
        else
        {
            await db.SortedSetAddAsync(PendingKey(message.Topic), message.Id, message.ExecutableTime.Ticks);
        }
    }

    public async Task PublishNewMessageAsync(Message message, object transaction, CancellationToken cancellationToken = default)
    {
        var db = transaction as IDatabase ?? GetDb();
        var json = JsonConvert.SerializeObject(message);
        var msgKey = MsgKey(message.Topic, message.Id);

        await db.StringSetAsync(msgKey, json, MsgExpiry);

        if (!string.IsNullOrEmpty(message.Queue))
        {
            await db.SortedSetAddAsync(PendingQueueKey(message.Topic, message.Queue), message.Id, message.ExecutableTime.Ticks);
            await db.SetAddAsync(QueuesKey(message.Topic), message.Queue);
        }
        else
        {
            await db.SortedSetAddAsync(PendingKey(message.Topic), message.Id, message.ExecutableTime.Ticks);
        }
    }

    public async Task PublishNewMessagesAsync(List<Message> messages)
    {
        var db = GetDb();
        var batch = db.CreateBatch();
        var tasks = new List<Task>(messages.Count * 2);

        foreach (var message in messages)
        {
            var json = JsonConvert.SerializeObject(message);
            var msgKey = MsgKey(message.Topic, message.Id);
            tasks.Add(batch.StringSetAsync(msgKey, json, MsgExpiry));

            if (!string.IsNullOrEmpty(message.Queue))
            {
                tasks.Add(batch.SortedSetAddAsync(PendingQueueKey(message.Topic, message.Queue), message.Id, message.ExecutableTime.Ticks));
                tasks.Add(batch.SetAddAsync(QueuesKey(message.Topic), message.Queue));
            }
            else
            {
                tasks.Add(batch.SortedSetAddAsync(PendingKey(message.Topic), message.Id, message.ExecutableTime.Ticks));
            }
        }

        batch.Execute();
        await Task.WhenAll(tasks);
    }

    public Task PublishNewMessagesAsync(List<Message> messages, object transaction)
    {
        return PublishNewMessagesAsync(messages);
    }

    #endregion

    #region Poll

    private static readonly LuaScript PollFromKeyScript = LuaScript.Prepare(
        @"
while true do
    local result = redis.call('ZRANGE', @pendingKey, 0, 0, 'WITHSCORES')
    if #result == 0 then return nil end
    local id = result[1]
    local score = tonumber(result[2])
    if score > tonumber(@nowTicks) then return nil end

    redis.call('ZREM', @pendingKey, id)

    local json = redis.call('GET', @msgKeyPrefix .. id)
    if json then
        local msg = cjson.decode(json)
        msg.Status = 1
        local newJson = cjson.encode(msg)
        redis.call('SET', @msgKeyPrefix .. id, newJson)
        redis.call('ZADD', @processingKey, @nowTicks, id)
        return newJson
    end
end"
    );

    private static readonly LuaScript PollGlobalScript = LuaScript.Prepare(
        @"
local function tryPoll(pendingKey)
    local result = redis.call('ZRANGE', pendingKey, 0, 0, 'WITHSCORES')
    if #result == 0 then return nil end
    local id = result[1]
    local score = tonumber(result[2])
    if score > tonumber(@nowTicks) then return nil end
    redis.call('ZREM', pendingKey, id)
    local json = redis.call('GET', @msgKeyPrefix .. id)
    if json then
        local msg = cjson.decode(json)
        msg.Status = 1
        local newJson = cjson.encode(msg)
        redis.call('SET', @msgKeyPrefix .. id, newJson)
        redis.call('ZADD', @processingKey, @nowTicks, id)
        return newJson
    end
    return ''
end

local res = tryPoll(@pendingKey)
if res then
    if res ~= '' then return res end
end

local queues = redis.call('SMEMBERS', @queuesKey)
for i, queue in ipairs(queues) do
    local qPendingKey = @queuesKeyPrefix .. queue
    res = tryPoll(qPendingKey)
    if res then
        if res ~= '' then return res end
    end
end

return nil"
    );

    public async Task<Message?> PollNewMessageAsync(string topic, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var nowTicks = DateTime.Now.Ticks;
        var json = await db.ScriptEvaluateAsync(PollGlobalScript,
            new
            {
                pendingKey = PendingKey(topic),
                processingKey = ProcessingKey(topic),
                msgKeyPrefix = MsgKeyPrefix(topic),
                queuesKey = QueuesKey(topic),
                queuesKeyPrefix = $"{_redisOptions.Value.KeyPrefix}:{topic}:pending:q:",
                nowTicks,
            });

        return DeserializeMessage((string?)json);
    }

    public async Task<Message?> PollNewMessageAsync(string topic, string? queue, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queue))
            return await PollNewMessageAsync(topic, cancellationToken);

        var db = GetDb();
        var nowTicks = DateTime.Now.Ticks;
        var json = await db.ScriptEvaluateAsync(PollFromKeyScript,
            new
            {
                pendingKey = PendingQueueKey(topic, queue),
                processingKey = ProcessingKey(topic),
                msgKeyPrefix = MsgKeyPrefix(topic),
                nowTicks,
            });

        return DeserializeMessage((string?)json);
    }

    private static readonly LuaScript PollFromKeyBatchScript = LuaScript.Prepare(
        @"
local result = {}
local total = tonumber(@count)
while true do
    local row = redis.call('ZRANGE', @pendingKey, 0, 0, 'WITHSCORES')
    if #row == 0 then break end
    local id = row[1]
    local score = tonumber(row[2])
    if score > tonumber(@nowTicks) then break end

    redis.call('ZREM', @pendingKey, id)

    local json = redis.call('GET', @msgKeyPrefix .. id)
    if json then
        local msg = cjson.decode(json)
        msg.Status = 1
        local newJson = cjson.encode(msg)
        redis.call('SET', @msgKeyPrefix .. id, newJson)
        redis.call('ZADD', @processingKey, @nowTicks, id)
        table.insert(result, newJson)
        if #result >= total then break end
    end
end
if #result == 0 then return nil end
return result"
    );

    private static readonly LuaScript PollGlobalBatchScript = LuaScript.Prepare(
        @"
local result = {}
local total = tonumber(@count)

local function tryPoll(pendingKey)
    while true do
        local row = redis.call('ZRANGE', pendingKey, 0, 0, 'WITHSCORES')
        if #row == 0 then break end
        local id = row[1]
        local score = tonumber(row[2])
        if score > tonumber(@nowTicks) then break end

        redis.call('ZREM', pendingKey, id)

        local json = redis.call('GET', @msgKeyPrefix .. id)
        if json then
            local msg = cjson.decode(json)
            msg.Status = 1
            local newJson = cjson.encode(msg)
            redis.call('SET', @msgKeyPrefix .. id, newJson)
            redis.call('ZADD', @processingKey, @nowTicks, id)
            table.insert(result, newJson)
            if #result >= total then return true end
        end
    end
    return false
end

local done = tryPoll(@pendingKey)
if done then return result end

local queues = redis.call('SMEMBERS', @queuesKey)
for i, queue in ipairs(queues) do
    local qPendingKey = @queuesKeyPrefix .. queue
    done = tryPoll(qPendingKey)
    if done then return result end
end

if #result == 0 then return nil end
return result"
    );

    public async Task<List<Message>> PollNewMessagesAsync(string topic, int count, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var nowTicks = DateTime.Now.Ticks;
        var json = await db.ScriptEvaluateAsync(PollGlobalBatchScript,
            new
            {
                pendingKey = PendingKey(topic),
                processingKey = ProcessingKey(topic),
                msgKeyPrefix = MsgKeyPrefix(topic),
                queuesKey = QueuesKey(topic),
                queuesKeyPrefix = $"{_redisOptions.Value.KeyPrefix}:{topic}:pending:q:",
                nowTicks,
                count,
            });

        return DeserializeMessages(json);
    }

    public async Task<List<Message>> PollNewMessagesAsync(string topic, string? queue, int count, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queue))
            return await PollNewMessagesAsync(topic, count, cancellationToken);

        var db = GetDb();
        var nowTicks = DateTime.Now.Ticks;
        var json = await db.ScriptEvaluateAsync(PollFromKeyBatchScript,
            new
            {
                pendingKey = PendingQueueKey(topic, queue),
                processingKey = ProcessingKey(topic),
                msgKeyPrefix = MsgKeyPrefix(topic),
                nowTicks,
                count,
            });

        return DeserializeMessages(json);
    }

    public async Task<List<string?>> PollAllQueuesAsync(string topic, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var queues = await db.SetMembersAsync(QueuesKey(topic));
        var result = new List<string?>();

        foreach (var queue in queues)
        {
            var qName = queue.ToString();
            var count = await db.SortedSetLengthAsync(PendingQueueKey(topic, qName));
            if (count > 0)
                result.Add(qName);
        }

        if (result.Count == 0)
            result.Add(null);

        return result;
    }

    #endregion

    #region Ack / Nack / Requeue / Retry

    public async Task AckMessageAsync(Message currentMessage, CancellationToken stoppingToken = default)
    {
        var db = GetDb();
        var msgKey = MsgKey(currentMessage.Topic, currentMessage.Id);
        var processingKey = ProcessingKey(currentMessage.Topic);

        currentMessage.Status = MessageStatus.Success;
        var json = JsonConvert.SerializeObject(currentMessage);

        var batch = db.CreateBatch();
        var t1 = batch.StringSetAsync(msgKey, json, MsgExpiry);
        var t2 = batch.SortedSetRemoveAsync(processingKey, currentMessage.Id);
        batch.Execute();
        await Task.WhenAll(t1, t2);
    }

    public async Task NackMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var msgKey = MsgKey(message.Topic, message.Id);
        var processingKey = ProcessingKey(message.Topic);

        message.Status = MessageStatus.Failed;
        var json = JsonConvert.SerializeObject(message);

        var batch = db.CreateBatch();
        var t1 = batch.StringSetAsync(msgKey, json, MsgExpiry);
        var t2 = batch.SortedSetRemoveAsync(processingKey, message.Id);
        batch.Execute();
        await Task.WhenAll(t1, t2);
    }

    public async Task RequeueMessageAsync(Message currentMessage)
    {
        var db = GetDb();
        var msgKey = MsgKey(currentMessage.Topic, currentMessage.Id);
        var processingKey = ProcessingKey(currentMessage.Topic);

        currentMessage.Status = MessageStatus.Waiting;
        currentMessage.ExecutableTime = DateTime.Now;
        var json = JsonConvert.SerializeObject(currentMessage);

        var batch = db.CreateBatch();
        var t1 = batch.StringSetAsync(msgKey, json, MsgExpiry);
        var t2 = batch.SortedSetRemoveAsync(processingKey, currentMessage.Id);
        batch.Execute();
        await Task.WhenAll(t1, t2);

        if (!string.IsNullOrEmpty(currentMessage.Queue))
            await db.SortedSetAddAsync(PendingQueueKey(currentMessage.Topic, currentMessage.Queue), currentMessage.Id, currentMessage.ExecutableTime.Ticks);
        else
            await db.SortedSetAddAsync(PendingKey(currentMessage.Topic), currentMessage.Id, currentMessage.ExecutableTime.Ticks);
    }

    public async Task UpdateRetryInfoAsync(Message message, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var msgKey = MsgKey(message.Topic, message.Id);
        var processingKey = ProcessingKey(message.Topic);

        message.Status = MessageStatus.Waiting;
        var json = JsonConvert.SerializeObject(message);

        var batch = db.CreateBatch();
        var t1 = batch.StringSetAsync(msgKey, json, MsgExpiry);
        var t2 = batch.SortedSetRemoveAsync(processingKey, message.Id);
        batch.Execute();
        await Task.WhenAll(t1, t2);

        if (!string.IsNullOrEmpty(message.Queue))
            await db.SortedSetAddAsync(PendingQueueKey(message.Topic, message.Queue), message.Id, message.ExecutableTime.Ticks);
        else
            await db.SortedSetAddAsync(PendingKey(message.Topic), message.Id, message.ExecutableTime.Ticks);
    }

    public async Task ResetMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var msgKey = MsgKey(message.Topic, message.Id);
        var processingKey = ProcessingKey(message.Topic);

        message.Status = MessageStatus.Waiting;
        var json = JsonConvert.SerializeObject(message);

        var batch = db.CreateBatch();
        var t1 = batch.StringSetAsync(msgKey, json, MsgExpiry);
        var t2 = batch.SortedSetRemoveAsync(processingKey, message.Id);
        batch.Execute();
        await Task.WhenAll(t1, t2);

        if (!string.IsNullOrEmpty(message.Queue))
            await db.SortedSetAddAsync(PendingQueueKey(message.Topic, message.Queue), message.Id, message.ExecutableTime.Ticks);
        else
            await db.SortedSetAddAsync(PendingKey(message.Topic), message.Id, message.ExecutableTime.Ticks);
    }

    #endregion

    #region Maintenance

    public async Task ResetOutOfDateMessagesAsync(string topic, DateTime executeTime, CancellationToken cancellationToken = default)
    {
        var db = GetDb();
        var processingKey = ProcessingKey(topic);
        var cutoffTicks = executeTime.Ticks;
        var nowTicks = DateTime.Now.Ticks;

        var expiredIds = await db.SortedSetRangeByScoreAsync(processingKey, double.NegativeInfinity, cutoffTicks);

        if (expiredIds.Length == 0)
            return;

        foreach (var id in expiredIds)
        {
            var idStr = id.ToString();
            var msgJson = (string?)await db.StringGetAsync(MsgKey(topic, idStr));
            var msg = msgJson != null ? JsonConvert.DeserializeObject<Message>(msgJson) : null;
            if (msg != null)
            {
                msg.Status = MessageStatus.Waiting;
                msg.ExecutableTime = DateTime.Now;
                await db.StringSetAsync(MsgKey(topic, idStr), JsonConvert.SerializeObject(msg), MsgExpiry);
                await db.SortedSetRemoveAsync(processingKey, idStr);
                if (!string.IsNullOrEmpty(msg.Queue))
                    await db.SortedSetAddAsync(PendingQueueKey(topic, msg.Queue), idStr, nowTicks);
                else
                    await db.SortedSetAddAsync(PendingKey(topic), idStr, nowTicks);
            }
        }
    }

    public Task ClearOldMessagesAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public Task InitTables(CancellationToken stoppingToken = default)
    {
        return Task.CompletedTask;
    }

    public Task<bool> TryAcquireResetLeaseAsync(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        // SET NX 原子抢占，key 自带 TTL，到期自动释放租约
        return GetDb().StringSetAsync(ResetLeaseKey, NodeId, duration, When.NotExists);
    }

    #endregion

    private static Message? DeserializeMessage(string? json) =>
        json == null ? null : JsonConvert.DeserializeObject<Message>(json);

    /// <summary>
    /// 批量领取脚本返回 Lua 数组（至少一条），这里解析为消息列表
    /// </summary>
    private static List<Message> DeserializeMessages(RedisResult? redisResult)
    {
        var messages = new List<Message>();
        if (redisResult == null || redisResult.IsNull)
            return messages;

        foreach (var value in (RedisResult[])redisResult!)
        {
            var json = (string?)value;
            if (json != null)
                messages.Add(JsonConvert.DeserializeObject<Message>(json)!);
        }

        return messages;
    }
}