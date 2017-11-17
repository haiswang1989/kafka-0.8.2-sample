使用low api进行消息的读取

一：什么时候使用这个接口
    1：多次读取消息
    2：读取Partition的部分消息
    3：事务管理，确保消息被消费有且仅有一次

二：代价
    1：自己管理offset
    2：自己查找Partition的leader broker
    3：自己leader broker的切换

三：SimpleConsumer的使用步骤
    1：必须知道读哪个topic的哪个Partition
    2：找到复制该Partition的leader broker，从而找到存有该Partition副本的broker
    3：自己写request并fetch数据
    4：识别和处理leader broker的change