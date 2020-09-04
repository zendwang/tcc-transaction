package org.mengyun.tcctransaction;


import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务
 * Created by changmingxie on 10/26/15.
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 7291423944314337931L;
    /**
     * 事务编号
     */
    private TransactionXid xid;
    /**
     * 事务状态
     */
    private TransactionStatus status;
    /**
     * 事务类型
     */
    private TransactionType transactionType;
    /**
     * 重试次数
     */
    private volatile int retriedCount = 0;
    /**
     * 创建时间
     */
    private Date createTime = new Date();
    /**
     * 最后更新时间
     */
    private Date lastUpdateTime = new Date();
    /**
     * 版本号
     */
    private long version = 1;
    /**
     * 参与者集合
     */
    private List<Participant> participants = new ArrayList<Participant>();
    /**
     * 附带属性映射
     */
    private Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();

    public Transaction() {

    }

    /**
     * 创建 分支事务
     * 该构成方法一般会在分支事务端被调用，用于根据
     * 从根事务端传递过来的事务上下文中创建一个分支事务
      * @param transactionContext
     */
    public Transaction(TransactionContext transactionContext) {
        this.xid = transactionContext.getXid(); // 事务上下文的 xid
        this.status = TransactionStatus.TRYING; //尝试中状态
        this.transactionType = TransactionType.BRANCH;//分支事务
    }

    /**
     * 创建指定类型的事务
     * 该构造方法通常会在根事务端被调用
     * @param transactionType 事务类型
     */
    public Transaction(TransactionType transactionType) {
        this.xid = new TransactionXid();
        this.status = TransactionStatus.TRYING;//尝试中状态
        this.transactionType = transactionType;
    }

    public Transaction(Object uniqueIdentity,TransactionType transactionType) {

        this.xid = new TransactionXid(uniqueIdentity);
        this.status = TransactionStatus.TRYING;
        this.transactionType = transactionType;
    }
    /**
     * 添加参与者
     */
    public void enlistParticipant(Participant participant) {
        participants.add(participant);
    }


    public Xid getXid() {
        return xid.clone();
    }

    public TransactionStatus getStatus() {
        return status;
    }


    public List<Participant> getParticipants() {
        return participants;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void changeStatus(TransactionStatus status) {
        this.status = status;
    }

    /**
     * 提交 TCC 事务
     */
    public void commit() {
        // 遍历所有参与者
        for (Participant participant : participants) {
            // 调用所有参与者的commit，这个参与者有本地参与者也有远程参与者
            participant.commit();
        }
    }
    /**
     * 回滚 TCC 事务
     */
    public void rollback() {
        for (Participant participant : participants) {
            participant.rollback();
        }
    }

    public int getRetriedCount() {
        return retriedCount;
    }

    public void addRetriedCount() {
        this.retriedCount++;
    }

    public void resetRetriedCount(int retriedCount) {
        this.retriedCount = retriedCount;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        this.version++;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date date) {
        this.lastUpdateTime = date;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void updateTime() {
        this.lastUpdateTime = new Date();
    }


}
