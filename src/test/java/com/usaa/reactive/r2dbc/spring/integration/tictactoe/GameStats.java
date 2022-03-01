package com.usaa.reactive.r2dbc.spring.integration.tictactoe;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Data @Table
public class GameStats implements Persistable<String> {
    @Id
    private String gameId;
    @Column("player_x")//, nullable=false, length=64)
    private String playerX;
    @Column("player_o")//, length=64)
    private String playerO;
    @Column//(length=64)
    private String winner;
    @Column//(length=2)
    private Space  lastMove;
    @Column
    private boolean over;
    @Transient
    private boolean isNew;

    @Override
    public String getId() {
        return gameId;
    }



    public enum Space {
        A1, A2, A3,
        B1, B2, B3,
        C1, C2, C3
    }
}
