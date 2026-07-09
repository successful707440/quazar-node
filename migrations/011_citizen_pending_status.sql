-- Citizen status: pending until passport issued; active after passport.
UPDATE citizens SET status = 'active' WHERE passport_issued = true;
UPDATE citizens SET status = 'pending' WHERE passport_issued = false AND name != 'successful';
