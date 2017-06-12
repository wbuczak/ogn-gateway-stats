package org.ogn.gateway.plugin.stats;

import static org.junit.Assert.assertTrue;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ogn.commons.beacon.AircraftBeacon;

/**
 * Copyright (c) 2015 OGN, All Rights Reserved.
 */

@Ignore
@RunWith(EasyMockRunner.class)
public class StatsTest {

	@Mock
	AircraftBeacon	b1	= null;

	@Mock
	AircraftBeacon	b2	= null;

	@Test
	public void test() throws Exception {

		Stats mrd = new Stats();
		mrd.init();

		Thread.sleep(Long.MAX_VALUE);

		assertTrue(true);

		// expect(b1.getId()).andReturn("DD1223").anyTimes();
		// expect(b1.getLat()).andReturn(1.0).anyTimes();
		// expect(b1.getLon()).andReturn(1.0).anyTimes();
		// expect(b1.getReceiverName()).andReturn("REC1").anyTimes();
		// long t = System.currentTimeMillis() - 15000;
		// expect(b1.getTimestamp()).andReturn(t).anyTimes();
		//
		// expect(b2.getId()).andReturn("DD1223").anyTimes();
		// expect(b2.getLat()).andReturn(1.2434).anyTimes();
		// expect(b2.getLon()).andReturn(0.54).anyTimes();
		// long t2 = t + 30000;
		// expect(b2.getTimestamp()).andReturn(t2).anyTimes();
		// expect(b2.getReceiverName()).andReturn("REC1").times(2);
		// expect(b2.getReceiverName()).andReturn("REC2").times(2);
		//
		// replay(b1, b2);
		//
		// MaxRangeDetector dd = new MaxRangeDetector();
		// dd.onBeacon(b1, null, "not important");
		// dd.onBeacon(b2, null, "not important");
		// dd.onBeacon(b2, null, "not important");
		// dd.onBeacon(b2, null, "not important");

	}
}
