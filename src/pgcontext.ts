import rx = require('rx');
import pg = require('pg');
import url = require('url');
import crypto = require('crypto');
import tx = require('./pgtx');
import log4js = require('log4js');
import nsql = require('nsql.js');
import decimal = require('decimal.js');
const logger = log4js.getLogger('pgcontext');


export class PGContext implements nsql.IContextWithTx {
    private pool: pg.Pool;
    constructor(private config: nsql.IContextConfig) {
        logger.debug(`pg params: ${JSON.stringify(config)}`);

        this.pool = new pg.Pool(this.config.params as pg.PoolConfig);

        this.pool.on('error', (e, client) => {
            logger.error(`pg client error\n${e.stack}`);
        });

        pg.types.setTypeParser(1231, (val) => {
            return new decimal(val).toNumber();
        });


        pg.types.setTypeParser(700, (val) => {
            return new decimal(val).toNumber();
        });

        pg.types.setTypeParser(701, (val) => {
            return new decimal(val).toNumber();
        });
    }

    public prepare<T>(stmt: string, ...params: any[]): rx.Observable<T> {

        const md5 = crypto.createHash('md5');

        md5.update(stmt);

        const name = md5.digest('hex');

        logger.debug(`prepare sql[${name}]: ${stmt}`);

        return rx.Observable.create<T>((observer) => {
            this.pool.connect()
                .then((client) => {
                    logger.debug(`prepare sql[${name}] get client -- success`);
                    return client.query({ name, text: stmt, values: params })
                        .then((result) => {
                            logger.debug(`prepare sql[${name}] exec -- success`);
                            for (const row of result.rows) {
                                observer.onNext(row);
                            }

                            if (result.command !== 'SELECT') {
                                observer.onNext(result.rowCount as any);
                            }

                            observer.onCompleted();
                            client.release();
                        })
                        .catch((error) => {
                            logger.error(`prepare sql[${name}] exec -- failed`, error);
                            client.release(error);
                            observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                        });
                })
                .catch((error) => {
                    logger.error(`prepare sql[${name}] exec -- failed`, error);
                    observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                });
        });
    }

    public exec<T>(sql: string): rx.Observable<T> {

        const md5 = crypto.createHash('md5');

        md5.update(sql);

        const name = md5.digest('hex');

        logger.debug(`exec sql[${name}]: ${sql}`);

        return rx.Observable.create<T>((observer) => {
            this.pool.connect()
                .then((client) => {
                    return client.query(sql)
                        .then((result) => {
                            logger.debug(`exec sql[${name}] exec -- success`);

                            for (const row of result.rows) {
                                logger.debug(JSON.stringify(row));
                                observer.onNext(row);
                            }

                            if (result.command !== 'SELECT') {
                                observer.onNext(result.rowCount as any);
                            }

                            observer.onCompleted();
                            client.release();
                        })
                        .catch((error) => {
                            logger.error(`exec sql[${name}] exec -- failed`, error);
                            client.release(error);
                            observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                        });
                })
                .catch((error) => {
                    logger.error(`exec sql[${name}] exec -- failed`, error);
                    observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                });
        });
    }

    public tx<T>(block: (context: nsql.ITx) => rx.Observable<T>): rx.Observable<T> {

        return rx.Observable.create<T>((observer) => {
            this.pool.connect()
                .then((client) => {
                    client.query('BEGIN').then(() => {
                        logger.debug(`BEGIN tx`);
                        const pgtx = new tx.PGTx(client);

                        let ob;

                        try {
                            ob = block(pgtx);
                        } catch (error) {
                            logger.error(`Rollback tx ...`, error);
                            pgtx.rollback()
                                .subscribe(
                                () => {
                                    logger.error(`Rollback tx -- success`);
                                }, (error2) => {
                                    logger.error(`Rollback tx -- failed`, error);
                                    observer.onError(error2);
                                }, () => {
                                    logger.error(`Rollback tx -- completed`);
                                    observer.onError(error);
                                });
                            return;
                        }

                        ob.subscribe((val) => {
                            observer.onNext(val);
                        }, (error) => {
                            logger.error(`Rollback tx ...`, error);
                            pgtx.rollbackWithError(error)
                                .subscribe(() => {
                                    logger.error(`Rollback tx -- success`);
                                }, (error2) => {
                                    logger.error(`Rollback tx -- failed`, error);
                                    observer.onError(error2);
                                }, () => {
                                    logger.error(`Rollback tx -- completed`);
                                    observer.onError(error);
                                });
                        }, () => {
                            logger.debug(`Commit tx ...`);
                            pgtx.commit()
                                .subscribe(() => {
                                    logger.debug(`Commit tx -- success`);
                                }, (error) => {
                                    logger.error(`Commit tx -- failed`, error);
                                    observer.onError(error);
                                }, () => {
                                    logger.debug(`Commit tx -- completed`);
                                    observer.onCompleted();
                                });
                        });
                    }).catch((error) => {
                        client.release(error);
                        observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                    });
                })
                .catch((error) => {
                    logger.debug(`start tx error`, error);
                    observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                });
        });
    }

    public procedure<T>(name: string, ...params: any[]): rx.Observable<T> {
        return rx.Observable.empty<T>();
    }
};