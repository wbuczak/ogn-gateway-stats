package org.ogn.gateway.plugin.stats;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ogn.commons.beacon.AircraftBeacon;
import org.ogn.commons.beacon.ReceiverBeacon;

/**
 * Copyright (c) 2015-2018 OGN, All Rights Reserved.
 */

// @Ignore
@RunWith(EasyMockRunner.class)
public class StatsTest {

	@Mock
	AircraftBeacon	b1	= null;

	@Mock
	ReceiverBeacon	r1	= null;

	@Test
	public void testReceptionRangeValidationAlgo() throws Exception {

		final Stats stats = new Stats();

		expect(b1.getAlt()).andReturn(55.3f);
		expect(r1.getAlt()).andReturn(220.3f);
		replay(b1, r1);

		assertFalse(stats.isValidDistance(151, b1, r1));
		assertTrue(stats.isValidDistance(148, b1, r1));

		reset(b1, r1);
		expect(b1.getAlt()).andReturn(1100.0f).anyTimes();
		expect(r1.getAlt()).andReturn(285.0f).anyTimes();
		replay(b1, r1);

		assertTrue(stats.isValidDistance(320, b1, r1));
		assertTrue(stats.isValidDistance(500, b1, r1));
	}
}
