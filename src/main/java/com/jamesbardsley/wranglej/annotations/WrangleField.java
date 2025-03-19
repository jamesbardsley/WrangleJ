package com.jamesbardsley.wranglej.annotations;

import com.jamesbardsley.wranglej.annotations.enums.WrangleFieldMode;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface WrangleField {
    WrangleFieldMode mode() default WrangleFieldMode.DIRECT;

    String direct() default "";
    String[] concatinationFields() default {};
    String arithmeticExpression() default "";
}