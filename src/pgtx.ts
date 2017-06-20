import rx = require('rx');
import nsql = require('nsql.js');
import pg = require('pg');
import crypto = require('crypto');
import log4js = require('log4js');

const logger = log4js.getLogger('PGTx');

export class PGTx implements nsql.ITx {
    private rollbacked = false;
    constructor(private client: pg.Client) {
    }

    public prepare<T>(stmt: string, ...params: any[]): rx.Observable<T> {
        if (this.rollbacked) {
            return rx.Observable.throw<T>(new Error('exec on rollback tx ....'));
        }

        const md5 = crypto.createHash('md5');

        md5.update(stmt);

        const name = md5.digest('hex');

        logger.debug(`prepare sql[${name}]: ${stmt} with params: ${JSON.stringify(params)}`);

        return rx.Observable.create<T>((observer) => {
            logger.debug(`prepare sql[${name}] get client -- success`);
            return this.client.query({ name, text: stmt, values: params })
                .then((result) => {
                    logger.debug(`prepare sql[${name}] exec -- success`);

                    for (const row of result.rows) {
                        observer.onNext(row);
                    }

                    if (result.command !== 'SELECT') {
                        observer.onNext(result.rowCount as any);
                    }

                    observer.onCompleted();
                })
                .catch((error) => {
                    logger.debug(`prepare sql[${name}] -- failed`, error);
                    observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                });
        });
    }

    public exec<T>(sql: string): rx.Observable<T> {
        if (this.rollbacked) {
            return rx.Observable.throw<T>(new Error('exec on rollback tx ....'));
        }

        const md5 = crypto.createHash('md5');

        md5.update(sql);

        const name = md5.digest('hex');

        logger.debug(`exec sql[${name}]: ${sql}`);

        return rx.Observable.create<T>((observer) => {
            return this.client.query(sql)
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
                })
                .catch((error) => {
                    logger.debug(`exec sql[${name}] -- failed`, error);
                    observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                });
        });
    }

    public commit<T>(t?: T): rx.Observable<T> {
        if (this.rollbacked) {
            return rx.Observable.empty<any>();
        }

        return rx.Observable.create<T>((observer) => {
            this.client.query('COMMIT')
                .then(() => {
                    this.client.release();
                    if (t) {
                        observer.onNext(t);
                    }
                    observer.onCompleted();
                    logger.debug(`commit -- success`);
                })
                .catch((error) => {
                    logger.debug(`commit -- failed`, error);
                    this.client.query('ROLLBACK')
                        .then(() => {
                            this.client.release();
                            observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                        }).catch((err2) => {
                            logger.error(`ROLLBACK error`, err2);
                            this.client.release(error);
                            observer.onError(new Error(`pgerror(${err2.code}):${err2.detail}`));
                        });
                });
        });
    }

    public rollback<T>(t?: T): rx.Observable<T> {
        if (this.rollbacked) {
            return rx.Observable.empty<any>();
        }

        this.rollbacked = true;
        return rx.Observable.create<T>((observer) => {
            this.client.query('ROLLBACK')
                .then(() => {
                    this.client.release();

                    if (t) {
                        observer.onNext(t);
                    }

                    observer.onCompleted();

                    logger.debug(`rollback -- success`);

                }).catch((error) => {
                    logger.error(`ROLLBACK error`, error);
                    this.client.release(error);
                    observer.onError(new Error(`pgerror(${error.code}):${error.detail}`));
                });
        });
    }

    public rollbackWithError(error: Error): rx.Observable<{}> {
        if (this.rollbacked) {
            return rx.Observable.empty<any>();
        }

        this.rollbacked = true;
        return rx.Observable.create((observer) => {
            this.client.query('ROLLBACK')
                .then(() => {
                    this.client.release(error);
                    observer.onCompleted();

                    logger.debug(`rollback -- success`);

                }).catch((error2) => {
                    logger.error(`ROLLBACK error`, error);
                    this.client.release(error2);
                    observer.onError(new Error(`pgerror(${error2.code}):${error2.detail}`));
                });
        });
    }

    public procedure<T>(name: string, ...params: any[]): rx.Observable<T> {
        return rx.Observable.empty<T>();
    }
};

