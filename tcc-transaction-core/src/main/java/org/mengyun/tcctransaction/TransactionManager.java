package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

/**
 * 事务管理器
 * 提供事务的获取、发起、提交、回滚，参与者的新增等等方法。
 * Created by changmingxie on 10/26/15.
 */
public class TransactionManager {

    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());
    //用于持久化事务日志
    private TransactionRepository transactionRepository;
    /**
     * 当前线程事务队列
     * Deque:double ended queue 双端队列
     * 用户保存该事务管理器上活动的事务
     */
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    private ExecutorService executorService;

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TransactionManager() {


    }

    public Transaction begin(Object uniqueIdentify) {
        Transaction transaction = new Transaction(uniqueIdentify,TransactionType.ROOT);
        transactionRepository.create(transaction);
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 发起 根事务
     * 事务处于 Try 阶段被调
     * @return
     */
    public Transaction begin() {
        //创建 根事务
        Transaction transaction = new Transaction(TransactionType.ROOT);
        //存储 事务，持久化事务日志
        transactionRepository.create(transaction);
        //注册 事务，将创建的事务保存在ThreadLocal类型的队列中
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 传播发起分支事务
     * 该方法在调用方法类型为 MethodType.PROVIDER 并且 事务处于 Try 阶段被调用
     * 用于从一个事务上下文中传播一个新事务，通常会在分支事务（比如dubbo中的provider端）的try阶段被调用
     * @param transactionContext 事务上下文
     * @return 分支事务
     */
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
        //创建 分支事务
        Transaction transaction = new Transaction(transactionContext);
        //存储 事务，事务日志 持久化
        transactionRepository.create(transaction);
        //注册 事务 到 事务管理器
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 传播获取分支事务
     * 传播发起分支事务。该方法在调用方法类型为 MethodType.PROVIDER 并且 事务处于 Confirm / Cancel 阶段被调用。
     * 用于从事务上下文中传播一个已存在的事务，通常会在分支事务（比如dubbo中的provider端）的confirm阶段和cancel阶段被调用
     * @param transactionContext
     * @return
     * @throws NoExistedTransactionException
     */
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        //查询 事务
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            //设置 事务 状态，设置事务状态为 CONFIRMING 或 CANCELLING
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            //注册事务到当前线程事务队列
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }

    /**
     * 提交事务
     * 该方法在事务处于 Confirm / Cancel 阶段被调用
     * @param asyncCommit
     */
    public void commit(boolean asyncCommit) {
        //获取 事务
        final Transaction transaction = getCurrentTransaction();
        //设置 事务状态 为 CONFIRMING
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        //更新 事务
        transactionRepository.update(transaction);
        //提交 事务，删除 事务
        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction);
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("compensable transaction async submit confirm failed, recovery job will try to confirm later.", commitException);
                throw new ConfirmingException(commitException);
            }
        } else {
            commitTransaction(transaction);
        }
    }

    /**
     * 回滚事务
     * @param asyncRollback
     */
    public void rollback(boolean asyncRollback) {
        //获取 事务
        final Transaction transaction = getCurrentTransaction();
        //设置 事务状态 为 CANCELLING
        transaction.changeStatus(TransactionStatus.CANCELLING);
        //更新 事务
        transactionRepository.update(transaction);
        //回滚 事务，删除 事务
        if (asyncRollback) {

            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                logger.warn("compensable transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancellingException(rollbackException);
            }
        } else {

            rollbackTransaction(transaction);
        }
    }


    private void commitTransaction(Transaction transaction) {
        try {
            //提交 事务
            transaction.commit();
            //从事务日志仓库中删除这个事务日志
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {
            logger.warn("compensable transaction confirm failed, recovery job will try to confirm later.", commitException);
            //转为抛出ConfirmingException异常，这样会导致事务在事务日志中不被删除，recovery会去处理长时间没有被删除的事务
            throw new ConfirmingException(commitException);
        }
    }

    private void rollbackTransaction(Transaction transaction) {
        try {
            transaction.rollback();
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {
            logger.warn("compensable transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }

    /**
     *  获取当前要处理的事务，此处要注意的是，
     *  队列的peek只是取出队列头部元素，但是不会将其删除。
     * @return
     */
    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek();//获取头部元素
        }
        return null;
    }

    /**
     * 判断当前事务管理中是否还有活动的事务
     * @return
     */
    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

    /**
     * 注册事务到 当前线程事务队列
     * @param transaction 事务
     */
    private void registerTransaction(Transaction transaction) {
        //如果队列还没有创建就先创建一个
        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }

        CURRENT.get().push(transaction);// 添加到头部
    }

    /**
     * 每次事务处理结束时，TCC框架都会调用该方法进行事务清理工作
     * 清理之前要比对要清理的事务是不是当前事务
     * @param transaction
     */
    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
                if (CURRENT.get().size() == 0) {
                    CURRENT.remove();
                }
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }

    /**
     * 往当前事务 添加一个事务参与者
     * @param participant
     */
    public void enlistParticipant(Participant participant) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.enlistParticipant(participant);//将参与者加入事务的参与者列表中
        transactionRepository.update(transaction);//更新事务日志
    }
}
