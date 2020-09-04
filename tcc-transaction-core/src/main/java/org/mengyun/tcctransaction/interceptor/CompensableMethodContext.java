package org.mengyun.tcctransaction.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.UniqueIdentity;
import org.mengyun.tcctransaction.common.MethodRole;
import org.mengyun.tcctransaction.support.FactoryBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Created by changming.xie on 04/04/19.
 */
public class CompensableMethodContext {

    ProceedingJoinPoint pjp = null;

    Method method = null;

    Compensable compensable = null;

    Propagation propagation = null;

    TransactionContext transactionContext = null;

    public CompensableMethodContext(ProceedingJoinPoint pjp) {
        this.pjp = pjp;
        this.method = getCompensableMethod();
        this.compensable = method.getAnnotation(Compensable.class);
        this.propagation = compensable.propagation();
        this.transactionContext = FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().get(pjp.getTarget(), method, pjp.getArgs());

    }

    public Compensable getAnnotation() {
        return compensable;
    }

    public Propagation getPropagation() {
        return propagation;
    }

    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public Method getMethod() {
        return method;
    }

    public Object getUniqueIdentity() {
        Annotation[][] annotations = this.getMethod().getParameterAnnotations();

        for (int i = 0; i < annotations.length; i++) {
            for (Annotation annotation : annotations[i]) {
                if (annotation.annotationType().equals(UniqueIdentity.class)) {

                    Object[] params = pjp.getArgs();
                    Object unqiueIdentity = params[i];

                    return unqiueIdentity;
                }
            }
        }

        return null;
    }


    private Method getCompensableMethod() {
        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();

        if (method.getAnnotation(Compensable.class) == null) {
            try {
                method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                return null;
            }
        }
        return method;
    }

    /**
     * 计算方法类型，根据不同方法类型，做不同的事务处理
     * 1. 方法类型为 MethodType.ROOT 时，发起根事务，判断条件如下二选一：
     * 1.1 事务传播级别为 Propagation.REQUIRED，并且当前没有事务。
     * 1.2 事务传播级别为 Propagation.REQUIRES_NEW，新建事务，如果当前存在事务，把当前事务挂起。此时，事务管理器的当前线程事务队列可能会存在多个事务。
     * 2. 方法类型为 MethodType.ROOT 时，发起分支事务，判断条件如下二选一：
     * 2.1 事务传播级别为 Propagation.REQUIRED，并且当前不存在事务，并且方法参数传递了事务上下文
     * 2.2 事务传播级别为 Propagation.MANDATORY，并且当前不存在事务，并且方法参数传递了事务上下文
     * 3. 方法类型为 MethodType.Normal 时，不进行事务处理。
     * @param isTransactionActive 是否事务开启
     * @return
     */
    public MethodRole getMethodRole(boolean isTransactionActive) {
        if ((propagation.equals(Propagation.REQUIRED) && !isTransactionActive && transactionContext == null) ||
                propagation.equals(Propagation.REQUIRES_NEW)) {
            return MethodRole.ROOT;
        } else if ((propagation.equals(Propagation.REQUIRED) || propagation.equals(Propagation.MANDATORY)) && !isTransactionActive && transactionContext != null) {
            return MethodRole.PROVIDER;
        } else {
            return MethodRole.NORMAL;
        }
    }

    public Object proceed() throws Throwable {
        return this.pjp.proceed();
    }
}