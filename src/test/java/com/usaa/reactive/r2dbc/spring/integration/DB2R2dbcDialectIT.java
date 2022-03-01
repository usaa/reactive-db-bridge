package com.usaa.reactive.r2dbc.spring.integration;

import com.usaa.reactive.r2dbc.sandbox.SandboxDB;
import com.usaa.reactive.r2dbc.spring.integration.tictactoe.GameStats;
import com.usaa.reactive.r2dbc.spring.integration.tictactoe.GameStatsRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.r2dbc.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
// We need to provide some value for the URL or the app won't start
// R2dbcConfiguration below will override with the "right" values before we try to connect
@DataR2dbcTest(properties = "spring.r2dbc.url=r2dbc:dummy://placeholder")
@AutoConfigurationPackage
public class DB2R2dbcDialectIT {
    @Autowired
    private GameStatsRepository repository;
    @Autowired
    private R2dbcEntityTemplate entityTemplate;

    @Configuration
    public static class R2dbcConfiguration {
        private static final SandboxDB DB = SandboxDB.getInstance();

        @PostConstruct
        public void startDBWithSpringLifecycle() {
            DB.startDB();
        }

        @PreDestroy
        public void stopDBWithSpringLifecycle() {
            DB.stopDB();
        }

        @Bean
        public ConnectionFactoryOptionsBuilderCustomizer customizer() {
            return builder -> builder.from(DB.getConnectionFactoryOptions());
        }
    }

    @BeforeEach
    public void initializeDatabase() {
        log.info("Resetting database table \"game_stats\"");

        String query = "DROP TABLE game_stats";
        entityTemplate.getDatabaseClient().sql(query).fetch().rowsUpdated().onErrorReturn(0).block();

        query = "CREATE TABLE game_stats (\n" +
                "    game_id VARCHAR(64) NOT NULL,\n" +
                "    player_x VARCHAR(64) NOT NULL,\n" +
                "    player_o VARCHAR(64),\n" +
                "    winner VARCHAR(64),\n" +
                "    last_move CHAR(2),\n" +
                "    over BOOLEAN,\n" +
                "    PRIMARY KEY(game_id)\n" +
                ")";
        entityTemplate.getDatabaseClient().sql(query).fetch().rowsUpdated().block();
    }


    @Test
    public void testCRUD() {
        Flux.range(1, 10)
                .map(this::createEntity)
                .flatMap(repository::save)
                .doOnNext(g -> log.info("Created game {}", g.getGameId()))
                .as(StepVerifier::create)
                .expectNextCount(10)
                .expectComplete()
                .verify();


        repository.count()
                .as(StepVerifier::create)
                .expectNext(10L)
                .expectComplete()
                .verify();

        repository.findAll()
                .as(StepVerifier::create)
                .expectNextCount(10)
                .expectComplete()
                .verify();

        for (int i = 1; i <= 10; i++) {
            String id = getId(i);
            repository.deleteById(id)
                    .doOnSuccess(v -> log.info("Deleted game {}", id))
                    .as(StepVerifier::create)
                    .expectComplete()
                    .verify();

            repository.count()
                    .doOnNext(c -> log.info("{} games remaining in DB", c))
                    .as(StepVerifier::create)
                    .expectNext(10L - i)
                    .expectComplete()
                    .verify();
        }
    }


    private GameStats createEntity(int gameNumber) {
        GameStats gameStats = new GameStats();
        gameStats.setNew(true);
        gameStats.setGameId(getId(gameNumber));
        gameStats.setPlayerX("Alice");
        gameStats.setPlayerO("Bob");
        if (gameNumber % 2 == 0) {
            gameStats.setOver(true);
            gameStats.setWinner("Alice");
            gameStats.setLastMove(GameStats.Space.A1);
        }
        return gameStats;
    }

    private String getId(int gameNumber) {
        return "test-game-" + gameNumber;
    }
}
