package com.usaa.reactive.r2dbc;

import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierDefault;
import java.lang.annotation.*;

/**
 * See: https://r2dbc.io/spec/0.8.6.RELEASE/spec/html/#introduction.requirements-conventions
 */
@Target(ElementType.PACKAGE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Nonnull
@TypeQualifierDefault({ElementType.METHOD, ElementType.PARAMETER})
public @interface NonNullApi {

}
