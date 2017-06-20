import { only, skip, slow, suite, test, timeout } from 'mocha-typescript';
import rx = require('rx');
import nsql = require('nsql.js');
import assert = require('assert');
import log4js = require('log4js');
import decimal = require('decimal.js');
const logger = log4js.getLogger('pgcontext');

interface IRow {
    v1: string;
    v2: number;
    v3: boolean;
    v4: any;
}

@suite('datasource test')
class DataSourceTest {
    @test('prepare test')
    public prepareTest(done: any) {
        const ds = new nsql.DataSource();

        const cnn = ds.get('pgtest');

        cnn.cacheSQL('test', 'insert into test(v1,v2,v3,v4) values($1,$2,$3,$4)');
        cnn.cacheProcedure('test', (context) => {
            return context.exec('select * from test');
        });

        cnn.prepare('@test', 'hello world', 2, true, { one: true, two: 'test' })
            .flatMap(() => {
                return cnn.procedure<IRow>('test');
            })
            .takeLast(1)
            .map((c) => {
                logger.debug(`${typeof c.v1}`);
                logger.debug(`${new decimal(c.v2).minus(1.99).toNumber()}`);
                logger.debug(`${typeof c.v3}`);
                logger.debug(`${typeof c.v4}`);
                return c;
            })
            // tslint:disable-next-line:no-empty
            .subscribe(() => { }, (error) => {
                done(error);
            }, () => {
                done();
            });
    }

    @test('tx test')
    public txTest(done: any) {
        const ds = new nsql.DataSource();

        const cnn = ds.get('pgtest');

        cnn.cacheSQL('test', 'insert into test(v1,v2,v3,v4) values($1,$2,$3,$4)');
        cnn.cacheProcedure('test', (context) => {
            return context.exec(`select * from test where v2=3`);
        });

        cnn.tx((context) => {
            return context.prepare('@test', 'hello world', 3, true, { one: true, two: 'test' })
                .flatMap(() => {
                    return context.rollback().count().flatMap(() => {
                        return cnn.procedure<IRow>('test');
                    });
                })
                .takeLast(1)
                .map((c) => {
                    logger.debug(`${typeof c.v1}`);
                    logger.debug(`${new decimal(c.v2).minus(1.99).toNumber()}`);
                    logger.debug(`${typeof c.v3}`);
                    logger.debug(`${typeof c.v4}`);
                    return c;
                });

        })
            // tslint:disable-next-line:no-empty
            .subscribe(() => { }, (error) => {
                done(error);
            }, () => {
                done();
            });
    }


};

