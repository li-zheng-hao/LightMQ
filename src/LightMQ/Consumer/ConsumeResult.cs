namespace LightMQ.Consumer;

public class ConsumeResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool IsSuccess { get; set; }
    
    /// <summary>
    /// 是否重新入队
    /// </summary>
    public bool Requeue { get; set; }
    
    static public ConsumeResult SuccessResult()
    {
        return new ConsumeResult()
        {
            IsSuccess = true,
            Requeue = false
        };
    }
    static public ConsumeResult RequeueResult()
    {
        return new ConsumeResult()
        {
            IsSuccess = true,
            Requeue = true
        };
    }
    static public ConsumeResult FailResult()
    {
        return new ConsumeResult()
        {
            IsSuccess = false,
            Requeue = false
        };
    }
}