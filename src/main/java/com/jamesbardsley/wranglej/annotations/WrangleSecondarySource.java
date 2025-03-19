package com.jamesbardsley.wranglej.annotations;

import com.jamesbardsley.wranglej.annotations.enums.WrangleJoinType;

public @interface WrangleSecondarySource {
    String name();
    String joinLeft();
    String joinRight();
    WrangleJoinType joinType();
}