package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;

import java.io.Serializable;

/**
 * 参与者
 * Created by changmingxie on 10/27/15.
 */
public class Participant implements Serializable {

    private static final long serialVersionUID = 4127729421281425247L;
    /**
     * 参与者事务编号
     */
    private TransactionXid xid;
    /**
     * 确认执行业务方法调用上下文
     */
    private InvocationContext confirmInvocationContext;
    /**
     * 取消执行业务方法调用上下文
     */
    private InvocationContext cancelInvocationContext;
    /**
     * 事务上下文编辑器
     */
    Class<? extends TransactionContextEditor> transactionContextEditorClass;

    public Participant() {

    }

    /**
     * 构造方法会在ResourceCoordinatorInterceptor切面中被调用
     * @param xid
     * @param confirmInvocationContext
     * @param cancelInvocationContext
     * @param transactionContextEditorClass
     */
    public Participant(TransactionXid xid, InvocationContext confirmInvocationContext, InvocationContext cancelInvocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        this.xid = xid;
        this.confirmInvocationContext = confirmInvocationContext;
        this.cancelInvocationContext = cancelInvocationContext;
        this.transactionContextEditorClass = transactionContextEditorClass;
    }

    public Participant(InvocationContext confirmInvocationContext, InvocationContext cancelInvocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        this.confirmInvocationContext = confirmInvocationContext;
        this.cancelInvocationContext = cancelInvocationContext;
        this.transactionContextEditorClass = transactionContextEditorClass;
    }

    public void setXid(TransactionXid xid) {
        this.xid = xid;
    }
    /**
     * 回滚参与者自己的事务
     */
    public void rollback() {
        Terminator.invoke(new TransactionContext(xid, TransactionStatus.CANCELLING.getId()), cancelInvocationContext, transactionContextEditorClass);
    }
    /**
     * 提交参与者自己的事务
     */
    public void commit() {
        // 会调用真正的commit方法（业务提供的）
        Terminator.invoke(new TransactionContext(xid, TransactionStatus.CONFIRMING.getId()), confirmInvocationContext, transactionContextEditorClass);
    }

    public TransactionXid getXid() {
        return xid;
    }

    public InvocationContext getConfirmInvocationContext() {
        return confirmInvocationContext;
    }

    public InvocationContext getCancelInvocationContext() {
        return cancelInvocationContext;
    }

}
