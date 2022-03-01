package com.usaa.reactive.r2dbc.spring.integration.tictactoe;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface GameStatsRepository extends ReactiveCrudRepository<GameStats, String>, SearchRepository {
    @Modifying
    @Query("UPDATE GameStats g SET g.playerO = ?2 WHERE g.gameId = ?1")
    Mono<Void> setPlayerO(String gameId, String playerO);

    @Modifying
    @Query("UPDATE GameStats g SET g.lastMove = ?2 WHERE g.gameId = ?1")
    Mono<Void> recordMove(String gameId, GameStats.Space space);

    @Modifying
    @Query("UPDATE GameStats g SET g.over = true, g.winner = ?2 WHERE g.gameId = ?1")
    Mono<Void> gameOver(String gameId, String winnerName);
}